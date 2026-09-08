import torch
import torch.nn as nn
import torch.nn.functional as F


# ============================================================
# B-SPLINE BASIS
# ============================================================

class BSplineBasis(nn.Module):
    """
    B-spline basis used by the v3 KAN-CVAE model.
    """

    def __init__(
        self,
        grid_size=8,
        spline_order=3
    ):
        super().__init__()

        self.grid_size = grid_size
        self.spline_order = spline_order

        self.n_basis = (
            grid_size + spline_order
        )

        n_knots = (
            self.n_basis
            + spline_order
            + 1
        )

        knots = torch.linspace(
            -spline_order / grid_size,
            1.0 + spline_order / grid_size,
            n_knots,
        )

        self.register_buffer(
            "knots",
            knots
        )

    def forward(self, x):

        x = torch.clamp(
            x,
            0.0,
            1.0
        )

        knots = self.knots
        degree = self.spline_order

        # ----------------------------------------------------
        # Degree 0
        # ----------------------------------------------------

        basis = (
            (
                x.unsqueeze(-1)
                >= knots[:-1]
            )
            &
            (
                x.unsqueeze(-1)
                < knots[1:]
            )
        ).float()

        # ----------------------------------------------------
        # Special case x == 1
        # ----------------------------------------------------

        basis[..., -1] = torch.where(
            x >= knots[-1],
            torch.ones_like(x),
            basis[..., -1],
        )

        # ----------------------------------------------------
        # Cox-de Boor recursion
        # ----------------------------------------------------

        for k in range(
            1,
            degree + 1
        ):

            left_den = (
                knots[k:-1]
                - knots[:-k-1]
            )

            right_den = (
                knots[k+1:]
                - knots[1:-k]
            )

            left_num = (
                x.unsqueeze(-1)
                - knots[:-k-1]
            )

            right_num = (
                knots[k+1:]
                - x.unsqueeze(-1)
            )

            left = torch.where(
                left_den != 0,
                left_num / left_den,
                torch.zeros_like(
                    left_num
                ),
            )

            right = torch.where(
                right_den != 0,
                right_num / right_den,
                torch.zeros_like(
                    right_num
                ),
            )

            basis = (
                left * basis[..., :-1]
                +
                right * basis[..., 1:]
            )

        return basis


# ============================================================
# KAN LINEAR
# ============================================================

class KANLinear(nn.Module):
    """
    v3 KAN-inspired spline linear layer.

    IMPORTANT:
    This preserves the normalization behavior of the
    existing v3 trained model.
    """

    def __init__(
        self,
        in_features,
        out_features,
        grid_size=8,
        spline_order=3,
    ):
        super().__init__()

        self.in_features = (
            in_features
        )

        self.out_features = (
            out_features
        )

        self.grid_size = (
            grid_size
        )

        self.spline_order = (
            spline_order
        )

        # ----------------------------------------------------
        # Base linear parameters
        # ----------------------------------------------------

        self.base_weight = nn.Parameter(
            torch.empty(
                out_features,
                in_features
            )
        )

        self.base_bias = nn.Parameter(
            torch.zeros(
                out_features
            )
        )

        # ----------------------------------------------------
        # B-spline basis
        # ----------------------------------------------------

        self.basis = BSplineBasis(
            grid_size=grid_size,
            spline_order=spline_order,
        )

        n_basis = (
            self.basis.n_basis
        )

        # ----------------------------------------------------
        # Spline parameters
        # ----------------------------------------------------

        self.spline_weight = nn.Parameter(
            torch.empty(
                out_features,
                in_features,
                n_basis,
            )
        )

        # ----------------------------------------------------
        # Initialization
        # ----------------------------------------------------

        nn.init.xavier_uniform_(
            self.base_weight
        )

        nn.init.normal_(
            self.spline_weight,
            mean=0.0,
            std=0.01,
        )

    def forward(self, x):

        # ----------------------------------------------------
        # IMPORTANT:
        # v3 model uses CURRENT BATCH min/max.
        # ----------------------------------------------------

        x_min = x.detach().amin(
            dim=0,
            keepdim=True,
        )

        x_max = x.detach().amax(
            dim=0,
            keepdim=True,
        )

        denom = (
            x_max
            - x_min
            + 1e-6
        )

        x_norm = (
            x - x_min
        ) / denom

        x_norm = torch.clamp(
            x_norm,
            0.0,
            1.0
        )

        # ----------------------------------------------------
        # Base path
        # ----------------------------------------------------

        base = F.silu(x)

        base_output = F.linear(
            base,
            self.base_weight,
            self.base_bias,
        )

        # ----------------------------------------------------
        # Spline path
        # ----------------------------------------------------

        basis = self.basis(
            x_norm
        )

        spline_output = torch.einsum(
            "bia,oia->bo",
            basis,
            self.spline_weight,
        )

        return (
            base_output
            + spline_output
        )


# ============================================================
# KAN ENCODER
# ============================================================

class KANEncoder(nn.Module):

    def __init__(
        self,
        input_dim,
        condition_dim,
        hidden_dim,
        latent_dim,
        grid_size=8,
        spline_order=3,
    ):
        super().__init__()

        total_input = (
            input_dim
            + condition_dim
        )

        self.layer1 = KANLinear(
            total_input,
            hidden_dim,
            grid_size=grid_size,
            spline_order=spline_order,
        )

        self.layer2 = KANLinear(
            hidden_dim,
            hidden_dim,
            grid_size=grid_size,
            spline_order=spline_order,
        )

        self.mu = nn.Linear(
            hidden_dim,
            latent_dim
        )

        self.logvar = nn.Linear(
            hidden_dim,
            latent_dim
        )

    def forward(
        self,
        x,
        condition
    ):

        # ----------------------------------------------------
        # Concatenate traffic + condition
        # ----------------------------------------------------

        h = torch.cat(
            [
                x,
                condition
            ],
            dim=1
        )

        # ----------------------------------------------------
        # KAN layers
        # ----------------------------------------------------

        h = self.layer1(
            h
        )

        h = self.layer2(
            h
        )

        # ----------------------------------------------------
        # Latent parameters
        # ----------------------------------------------------

        mu = self.mu(
            h
        )

        logvar = self.logvar(
            h
        )

        # ----------------------------------------------------
        # Prevent numerical instability
        # ----------------------------------------------------

        logvar = torch.clamp(
            logvar,
            min=-10.0,
            max=10.0,
        )

        return (
            mu,
            logvar
        )


# ============================================================
# KAN DECODER
# ============================================================

class KANDecoder(nn.Module):

    def __init__(
        self,
        latent_dim,
        condition_dim,
        hidden_dim,
        output_dim,
        grid_size=8,
        spline_order=3,
    ):
        super().__init__()

        total_input = (
            latent_dim
            + condition_dim
        )

        self.layer1 = KANLinear(
            total_input,
            hidden_dim,
            grid_size=grid_size,
            spline_order=spline_order,
        )

        self.layer2 = KANLinear(
            hidden_dim,
            hidden_dim,
            grid_size=grid_size,
            spline_order=spline_order,
        )

        self.output = nn.Linear(
            hidden_dim,
            output_dim
        )

    def forward(
        self,
        z,
        condition
    ):

        h = torch.cat(
            [
                z,
                condition
            ],
            dim=1
        )

        h = self.layer1(
            h
        )

        h = self.layer2(
            h
        )

        return self.output(
            h
        )


# ============================================================
# KAN-CVAE
# ============================================================

class KANCVAE(nn.Module):

    def __init__(
        self,
        input_dim,
        condition_dim,
        hidden_dim,
        latent_dim,
        grid_size=8,
        spline_order=3,
    ):
        super().__init__()

        self.encoder = KANEncoder(
            input_dim=input_dim,
            condition_dim=condition_dim,
            hidden_dim=hidden_dim,
            latent_dim=latent_dim,
            grid_size=grid_size,
            spline_order=spline_order,
        )

        self.decoder = KANDecoder(
            latent_dim=latent_dim,
            condition_dim=condition_dim,
            hidden_dim=hidden_dim,
            output_dim=input_dim,
            grid_size=grid_size,
            spline_order=spline_order,
        )

    # --------------------------------------------------------
    # VAE reparameterization
    # --------------------------------------------------------

    def reparameterize(
        self,
        mu,
        logvar
    ):

        std = torch.exp(
            0.5 * logvar
        )

        eps = torch.randn_like(
            std
        )

        return (
            mu
            + eps * std
        )

    # --------------------------------------------------------
    # Forward
    # --------------------------------------------------------

    def forward(
        self,
        x,
        condition
    ):

        mu, logvar = (
            self.encoder(
                x,
                condition
            )
        )

        z = self.reparameterize(
            mu,
            logvar
        )

        reconstruction = (
            self.decoder(
                z,
                condition
            )
        )

        return (
            reconstruction,
            mu,
            logvar
        )

    # --------------------------------------------------------
    # Reconstruction
    # --------------------------------------------------------

    @torch.no_grad()
    def reconstruct(
        self,
        x,
        condition
    ):

        reconstruction, _, _ = (
            self.forward(
                x,
                condition
            )
        )

        return reconstruction


# ============================================================
# MODEL BUILDER
# ============================================================

def build_model(
    input_dim,
    hidden_dim,
    latent_dim,
    grid_size,
    spline_order,
    condition_dim,
):
    """
    Build the v3 KAN-CVAE model.
    """

    model = KANCVAE(

        input_dim=input_dim,

        condition_dim=condition_dim,

        hidden_dim=hidden_dim,

        latent_dim=latent_dim,

        grid_size=grid_size,

        spline_order=spline_order,
    )

    return model