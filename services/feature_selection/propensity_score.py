import pandas as pd

from config import (
    MIN_GROUP_SIZE,
    MAX_CMB_CONDITIONING_COLUMNS
)

from logger import IDSLogger


class PropensityScore:

    def __init__(
        self,
        min_group_size=MIN_GROUP_SIZE,
        max_cmb=MAX_CMB_CONDITIONING_COLUMNS
    ):

        self.min_group_size = int(min_group_size)
        self.max_cmb = int(max_cmb)

        self.logger = IDSLogger().get_logger()

    # ==========================================================
    # Remove Duplicate Columns
    # ==========================================================

    def _remove_duplicate_columns(self, dataframe):

        if dataframe is None:
            return dataframe

        if dataframe.columns.duplicated().any():

            duplicate_columns = (
                dataframe.columns[
                    dataframe.columns.duplicated()
                ]
                .tolist()
            )

            self.logger.warning(
                f"Duplicate dataframe columns detected: "
                f"{duplicate_columns}"
            )

            dataframe = dataframe.loc[
                :,
                ~dataframe.columns.duplicated(
                    keep="first"
                )
            ].copy()

        return dataframe

    # ==========================================================
    # Remove Duplicate List Values
    # ==========================================================

    def _unique_columns(self, columns):

        if not columns:
            return []

        return list(
            dict.fromkeys(columns)
        )

    # ==========================================================
    # Prepare Conditioning Set
    # ==========================================================

    def prepare_cmb(
        self,
        dataframe,
        cmb
    ):

        if dataframe is None:
            return []

        if not cmb:
            return []

        valid_columns = [
            col
            for col in cmb
            if col in dataframe.columns
        ]

        # Remove duplicates
        valid_columns = self._unique_columns(
            valid_columns
        )

        # ------------------------------------------------------
        # Engineering limitation
        #
        # Prevent extremely sparse high-dimensional
        # conditional grouping.
        # ------------------------------------------------------

        if len(valid_columns) <= self.max_cmb:

            return valid_columns

        self.logger.warning(
            f"CMB size ({len(valid_columns)}) "
            f"exceeds MAX_CMB_CONDITIONING_COLUMNS "
            f"({self.max_cmb}); "
            f"using the most recent conditioning columns "
            f"for empirical estimation."
        )

        return valid_columns[
            -self.max_cmb:
        ]

    # ==========================================================
    # Conditional Distributions
    #
    # Calculates:
    #
    # P(Y | X, CMB)
    #
    # ==========================================================

    def conditional_distributions(
        self,
        dataframe,
        feature,
        target,
        cmb
    ):

        # ======================================================
        # Basic validation
        # ======================================================

        if dataframe is None:
            return {}

        if dataframe.empty:
            return {}

        if feature == target:
            return {}

        if feature not in dataframe.columns:
            return {}

        if target not in dataframe.columns:
            return {}

        # ======================================================
        # Remove duplicate dataframe columns
        # ======================================================

        dataframe = (
            self._remove_duplicate_columns(
                dataframe
            )
        )

        # After removing duplicates, verify again
        if feature not in dataframe.columns:
            return {}

        if target not in dataframe.columns:
            return {}

        # ======================================================
        # Prepare CMB
        # ======================================================

        cmb_columns = self.prepare_cmb(
            dataframe,
            cmb
        )

        # ------------------------------------------------------
        # Feature must not appear in CMB
        # Target must not appear in CMB
        # ------------------------------------------------------

        cmb_columns = [
            col
            for col in cmb_columns
            if col != feature
            and col != target
        ]

        cmb_columns = self._unique_columns(
            cmb_columns
        )

        # ======================================================
        # Build grouping columns
        # ======================================================

        group_columns = (
            cmb_columns + [feature]
        )

        group_columns = self._unique_columns(
            group_columns
        )

        # ======================================================
        # Required columns
        # ======================================================

        required_columns = (
            group_columns + [target]
        )

        required_columns = self._unique_columns(
            required_columns
        )

        # ======================================================
        # Validate required columns
        # ======================================================

        missing_columns = [
            column
            for column in required_columns
            if column not in dataframe.columns
        ]

        if missing_columns:

            self.logger.warning(
                f"Skipping feature '{feature}'. "
                f"Missing columns: {missing_columns}"
            )

            return {}

        # ======================================================
        # IMPORTANT
        #
        # Create a completely independent dataframe containing
        # only the columns required by this calculation.
        #
        # This prevents duplicate column/index collisions
        # during reset_index().
        # ======================================================

        work_df = dataframe[
            required_columns
        ].copy()

        # ======================================================
        # Final duplicate-column protection
        # ======================================================

        work_df = (
            self._remove_duplicate_columns(
                work_df
            )
        )

        # ======================================================
        # Group:
        #
        # CMB + X + Y
        #
        # and calculate frequency.
        # ======================================================

        try:

            grouped = (
                work_df
                .groupby(
                    group_columns + [target],
                    observed=True,
                    dropna=False
                )
                .size()
                .rename("count")
                .reset_index()
            )

        except ValueError as error:

            self.logger.error(
                f"Grouping error for feature "
                f"'{feature}': {error}"
            )

            return {}

        except Exception as error:

            self.logger.exception(
                f"Unexpected grouping error for "
                f"'{feature}': {error}"
            )

            return {}

        if grouped.empty:
            return {}

        # ======================================================
        # Remove groups smaller than MIN_GROUP_SIZE
        # ======================================================

        try:

            cell_totals = (
                grouped
                .groupby(
                    group_columns,
                    observed=True,
                    dropna=False
                )["count"]
                .transform("sum")
            )

        except Exception as error:

            self.logger.exception(
                f"Cell-total calculation failed "
                f"for '{feature}': {error}"
            )

            return {}

        grouped = grouped[
            cell_totals >= self.min_group_size
        ].copy()

        if grouped.empty:
            return {}

        # ======================================================
        # Calculate:
        #
        # P(Y | X, CMB)
        #
        # ======================================================

        try:

            denominators = (
                grouped
                .groupby(
                    group_columns,
                    observed=True,
                    dropna=False
                )["count"]
                .transform("sum")
            )

            grouped["probability"] = (
                grouped["count"]
                / denominators
            )

        except Exception as error:

            self.logger.exception(
                f"Probability calculation failed "
                f"for '{feature}': {error}"
            )

            return {}

        # ======================================================
        # Build nested distribution
        #
        # {
        #     CMB_value: {
        #
        #         X_value: {
        #             Y_value: probability
        #         }
        #
        #     }
        # }
        #
        # ======================================================

        result = {}

        try:

            grouped_iterator = grouped.groupby(
                group_columns,
                observed=True,
                dropna=False
            )

            for group_values, group in grouped_iterator:

                # ----------------------------------------------
                # Convert scalar to tuple
                # ----------------------------------------------

                if not isinstance(
                    group_values,
                    tuple
                ):

                    group_values = (
                        group_values,
                    )

                # ----------------------------------------------
                # Separate CMB and feature value
                # ----------------------------------------------

                if cmb_columns:

                    cmb_key = tuple(
                        group_values[:-1]
                    )

                    x_value = (
                        group_values[-1]
                    )

                else:

                    cmb_key = ()

                    x_value = (
                        group_values[0]
                    )

                # ----------------------------------------------
                # Build P(Y | X,CMB)
                # ----------------------------------------------

                distribution = {}

                for _, row in group.iterrows():

                    y_value = row[target]

                    probability = float(
                        row["probability"]
                    )

                    distribution[
                        y_value
                    ] = probability

                if not distribution:
                    continue

                if cmb_key not in result:

                    result[cmb_key] = {}

                result[
                    cmb_key
                ][
                    x_value
                ] = distribution

        except Exception as error:

            self.logger.exception(
                f"Distribution construction failed "
                f"for '{feature}': {error}"
            )

            return {}

        # ======================================================
        # Important for Eq. 13:
        #
        # Need at least two X values under the same CMB.
        # ======================================================

        result = {
            cmb_key: per_x
            for cmb_key, per_x
            in result.items()
            if len(per_x) >= 2
        }

        return result

    # ==========================================================
    # Calculate All Candidate Features
    # ==========================================================

    def calculate_all_fast(
        self,
        dataframe,
        features,
        target,
        cmb
    ):

        distributions = {}

        # ======================================================
        # Validation
        # ======================================================

        if dataframe is None:
            return distributions

        if dataframe.empty:
            return distributions

        if not features:
            return distributions

        if target not in dataframe.columns:
            self.logger.warning(
                f"Target '{target}' not found."
            )

            return distributions

        # ======================================================
        # Remove duplicate dataframe columns ONCE
        #
        # This is faster than doing it repeatedly.
        # ======================================================

        dataframe = (
            self._remove_duplicate_columns(
                dataframe
            )
        )

        # ======================================================
        # Remove duplicate candidate features
        # ======================================================

        features = self._unique_columns(
            features
        )

        # ======================================================
        # Never allow target to become X
        # ======================================================

        candidate_features = [
            feature
            for feature in features
            if feature != target
            and feature in dataframe.columns
        ]

        # ======================================================
        # Calculate conditional distributions
        # ======================================================

        for feature in candidate_features:

            distributions[feature] = (
                self.conditional_distributions(
                    dataframe,
                    feature,
                    target,
                    cmb
                )
            )

        return distributions