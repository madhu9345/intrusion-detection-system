// IR-IDS Defense Operations Console JavaScript

let isStreamActive = true;
let streamInterval = null;
let pipelineStagesData = [];

// Sliders configuration for the simulator
const KEY_SIMULATOR_FEATURES = [
    { key: "flow_packets_per_sec", label: "Packets/sec (scaled)", min: 0, max: 100, step: 0.5, default: 0.5 },
    { key: "flow_duration", label: "Flow Duration (scaled)", min: 0, max: 100, step: 0.5, default: 4.2 },
    { key: "Down/Up Ratio", label: "Down/Up Ratio", min: 0, max: 10, step: 0.1, default: 1.0 },
    { key: "Bwd IAT Min", label: "Bwd IAT Min (scaled)", min: 0, max: 50, step: 0.5, default: 8.0 },
    { key: "Fwd IAT Min", label: "Fwd IAT Min (scaled)", min: 0, max: 50, step: 0.5, default: 6.0 },
    { key: "Pkt Size Avg", label: "Avg Packet Size (scaled)", min: 0, max: 30, step: 0.2, default: 5.2 },
    { key: "total_forward_packets", label: "Total Fwd Packets", min: 1, max: 1000, step: 1, default: 2 },
    { key: "Idle Mean", label: "Idle Mean (scaled)", min: 0, max: 50, step: 0.5, default: 0 }
];

document.addEventListener("DOMContentLoaded", () => {
    initNavigationTabs();
    initSimulatorControls();
    loadPipelineData();
    fetchTelemetry();
    startStreamInterval();

    const btnToggle = document.getElementById("btnToggleStream");
    if (btnToggle) {
        btnToggle.addEventListener("click", toggleStreaming);
    }

    const btnRunSim = document.getElementById("btnRunSimulation");
    if (btnRunSim) {
        btnRunSim.addEventListener("click", runSimulation);
    }
});

// ============================================================
// NAVIGATION TABS
// ============================================================
function initNavigationTabs() {
    const tabs = document.querySelectorAll(".nav-tab");
    tabs.forEach(tab => {
        tab.addEventListener("click", () => {
            tabs.forEach(t => t.classList.remove("active"));
            tab.classList.add("active");

            const targetTabId = "tab-" + tab.getAttribute("data-tab");
            document.querySelectorAll(".view-tab").forEach(v => v.classList.remove("active"));
            const targetEl = document.getElementById(targetTabId);
            if (targetEl) targetEl.classList.add("active");
        });
    });
}

// ============================================================
// TELEMETRY POLLING (1-SECOND REAL-TIME INTERVAL)
// ============================================================
function startStreamInterval() {
    if (streamInterval) clearInterval(streamInterval);
    streamInterval = setInterval(() => {
        if (isStreamActive) {
            fetchTelemetry();
        }
    }, 1000);
}

function toggleStreaming() {
    isStreamActive = !isStreamActive;
    const btn = document.getElementById("btnToggleStream");
    if (!btn) return;
    if (isStreamActive) {
        btn.innerHTML = `<span class="stream-icon">▶</span> Live`;
        btn.style.borderColor = "rgba(16, 185, 129, 0.3)";
        btn.style.color = "var(--accent-green)";
    } else {
        btn.innerHTML = `<span class="stream-icon">⏸</span> Paused`;
        btn.style.borderColor = "rgba(245, 158, 11, 0.4)";
        btn.style.color = "var(--accent-amber)";
    }
}

async function fetchTelemetry() {
    try {
        const [statsRes, eventsRes, alertsRes] = await Promise.all([
            fetch("/api/stats"),
            fetch("/api/events"),
            fetch("/api/alerts")
        ]);

        if (statsRes.ok) {
            const data = await statsRes.json();
            updateStatsUI(data.stats);
        }
        if (eventsRes.ok) {
            const events = await eventsRes.json();
            updateEventsTable(events);
        }
        if (alertsRes.ok) {
            const alerts = await alertsRes.json();
            updateAlertsList(alerts);
        }
    } catch (err) {
        console.warn("Telemetry fetch error:", err);
    }
}

function updateStatsUI(stats) {
    if (!stats) return;
    const totalEl = document.getElementById("kpiTotalFlows");
    const rateEl = document.getElementById("kpiRate");
    const benignEl = document.getElementById("kpiBenignFlows");
    const benignPctEl = document.getElementById("kpiBenignPct");
    const attacksEl = document.getElementById("kpiKnownAttacks");
    const attacksSubEl = document.getElementById("kpiAttacksSub");
    const zeroDayEl = document.getElementById("kpiZeroDay");
    const avgConfEl = document.getElementById("kpiAvgConf");

    if (totalEl) totalEl.innerText = Number(stats.total_flows).toLocaleString();
    if (rateEl) rateEl.innerText = stats.ingestion_rate > 0 ? `▲ ${stats.ingestion_rate}` : '--';
    if (benignEl) benignEl.innerText = Number(stats.benign_flows).toLocaleString();
    
    if (benignPctEl) {
        if (stats.total_flows > 0 && stats.benign_flows > 0) {
            const pct = Math.min(100, (stats.benign_flows / stats.total_flows) * 100).toFixed(1);
            benignPctEl.innerText = `${pct}% of Total Traffic`;
        } else {
            benignPctEl.innerText = "0.0% of Total Traffic";
        }
    }

    if (attacksEl) attacksEl.innerText = Number(stats.known_attacks).toLocaleString();
    if (attacksSubEl) attacksSubEl.innerText = `${Number(stats.known_attacks).toLocaleString()} Attacks Filtered`;
    if (zeroDayEl) zeroDayEl.innerText = Number(stats.zero_day_alerts).toLocaleString();
    if (avgConfEl) avgConfEl.innerText = stats.avg_confidence > 0 ? `${stats.avg_confidence}%` : '--%';

    const brokerText = document.getElementById("brokerText");
    const brokerStatus = document.getElementById("brokerStatus");
    if (brokerText && brokerStatus) {
        if (stats.kafka_connected) {
            brokerText.innerText = `Kafka Live (${stats.ingestion_rate || 500} flows/s)`;
            brokerStatus.className = "status-indicator online";
        } else {
            brokerText.innerText = "Connecting to Kafka cluster...";
            brokerStatus.className = "status-indicator offline";
        }
    }
}

let lastFirstEventId = null;

function updateEventsTable(events) {
    const tbody = document.getElementById("eventTableBody");
    if (!tbody || !events) return;

    if (events.length === 0) {
        tbody.innerHTML = `<tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 2.5rem; font-size: 0.9rem;">Waiting for live network packets from Suricata pipeline...</td></tr>`;
        return;
    }

    tbody.innerHTML = events.slice(0, 15).map((ev, idx) => {
        const isBenign = ev.prediction === "BENIGN";
        const pillClass = isBenign ? "benign" : "attack";
        const isNew = idx === 0 && ev.id !== lastFirstEventId;
        const rowStyle = isNew ? "background: rgba(6, 182, 212, 0.08); transition: background 1s ease;" : "";
        return `
            <tr style="${rowStyle}">
                <td style="color: var(--text-muted); font-size: 0.8rem;">${ev.timestamp}</td>
                <td><span style="color: var(--accent-cyan); font-family: monospace;">${ev.src}</span> ➔ <span style="color: var(--text-secondary); font-family: monospace;">${ev.dst}</span></td>
                <td><span style="color: #cbd5e1; font-weight: 500;">${ev.proto || "TCP"}</span></td>
                <td><span class="pill-class ${pillClass}">${ev.prediction}</span></td>
                <td><strong style="color: ${ev.confidence >= 80 ? 'var(--accent-green)' : 'var(--accent-amber)'}">${ev.confidence}%</strong></td>
                <td><span style="color: var(--text-secondary); font-family: monospace; font-size: 0.8rem;">${ev.top_shap_feature || "flow_packets_per_sec"} (${ev.top_shap_val ?? 0})</span></td>
                <td><span style="font-size: 0.7rem; color: var(--text-muted);">${ev.route}</span></td>
            </tr>
        `;
    }).join("");

    if (events.length > 0) {
        lastFirstEventId = events[0].id;
    }
}

function updateAlertsList(alerts) {
    const list = document.getElementById("alertsList");
    if (!list || !alerts) return;

    if (alerts.length === 0) {
        list.innerHTML = `<div style="color: var(--text-muted); font-size: 0.85rem; padding: 1rem;">No zero-day anomaly events recorded. Normal profile.</div>`;
        return;
    }

    list.innerHTML = alerts.slice(0, 5).map(a => `
        <div class="alert-item">
            <div class="alert-top">
                <span class="alert-type">&#9888; ${a.anomaly_type || "Novel Zero-Day Threat"}</span>
                <span class="alert-time">${a.timestamp}</span>
            </div>
            <div class="alert-details">
                <div><span>Origin:</span> <strong style="color: #fff;">${a.src}</strong> ➔ ${a.dst}</div>
                <div><span>Reconstruction Loss:</span> <strong class="alert-loss-tag">${a.error}</strong> (EVT Limit: ${a.threshold})</div>
            </div>
        </div>
    `).join("");
}

// ============================================================
// PIPELINE WORKFLOW CARDS & MODAL
// ============================================================
async function loadPipelineData() {
    try {
        const res = await fetch("/api/pipeline");
        if (res.ok) {
            pipelineStagesData = await res.json();
            renderStageCards(pipelineStagesData);
        }
    } catch (e) {
        console.warn("Error loading pipeline:", e);
    }
}

function renderStageCards(stages) {
    const container = document.getElementById("stageCardsContainer");
    if (!container) return;

    container.innerHTML = stages.map(s => `
        <div class="stage-card" onclick="openStageModal(${s.stage})">
            <div class="stage-card-num">STAGE 0${s.stage}</div>
            <h4>${s.name}</h4>
            <p>${s.description}</p>
            <div class="stage-card-footer">
                <span style="color: var(--accent-cyan);">${s.output_topic || s.output_topics?.join(', ')}</span>
                <span class="stage-status-badge">${s.status}</span>
            </div>
        </div>
    `).join("");
}

window.openStageModal = function(stageNum) {
    const stage = pipelineStagesData.find(s => s.stage === stageNum);
    if (!stage) return;

    document.getElementById("modalStageTag").innerText = `STAGE 0${stage.stage} &bull; ${stage.type}`;
    document.getElementById("modalStageTitle").innerText = stage.name;

    const body = document.getElementById("modalStageBody");
    body.innerHTML = `
        <p style="margin-bottom: 1rem;">${stage.description}</p>
        <div class="modal-kv">
            <div class="modal-kv-key">Input:</div>
            <div class="modal-kv-val">${stage.input || stage.input_topic || "Network Packet Stream"}</div>
        </div>
        <div class="modal-kv">
            <div class="modal-kv-key">Output Topic:</div>
            <div class="modal-kv-val" style="color: var(--accent-cyan);">${stage.output_topic || stage.output_topics?.join(', ')}</div>
        </div>
        ${stage.model_name ? `
        <div class="modal-kv">
            <div class="modal-kv-key">Model / Engine:</div>
            <div class="modal-kv-val" style="color: var(--accent-green);">${stage.model_name}</div>
        </div>` : ''}
        ${stage.threshold ? `
        <div class="modal-kv">
            <div class="modal-kv-key">Threshold:</div>
            <div class="modal-kv-val" style="color: var(--accent-purple);">${stage.threshold}</div>
        </div>` : ''}
        ${stage.throughput ? `
        <div class="modal-kv">
            <div class="modal-kv-key">Throughput:</div>
            <div class="modal-kv-val">${stage.throughput}</div>
        </div>` : ''}
    `;

    document.getElementById("stageModal").classList.add("open");
};

window.closeStageModal = function() {
    document.getElementById("stageModal").classList.remove("open");
};

// ============================================================
// SIMULATOR & INTERACTIVE TESTING
// ============================================================
function initSimulatorControls() {
    const container = document.getElementById("featureInputsContainer");
    if (!container) return;

    container.innerHTML = KEY_SIMULATOR_FEATURES.map(f => `
        <div class="slider-group">
            <div class="slider-header">
                <span class="slider-label">${f.label}</span>
                <span class="slider-val" id="val-${f.key}">${f.default}</span>
            </div>
            <input type="range" class="feature-slider" id="slider-${f.key}"
                min="${f.min}" max="${f.max}" step="${f.step}" value="${f.default}"
                oninput="document.getElementById('val-${f.key}').innerText = this.value">
        </div>
    `).join("");

    // Preset buttons click listeners
    const presetBtns = document.querySelectorAll(".btn-preset");
    presetBtns.forEach(btn => {
        btn.addEventListener("click", () => {
            presetBtns.forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            const presetName = btn.getAttribute("data-preset");
            applyPresetToSliders(presetName);
            runSimulation();
        });
    });

    // Run first simulation on load
    runSimulation();
}

const PRESET_SLIDER_DEFAULTS = {
    "BENIGN": {
        "flow_packets_per_sec": 0.5, "flow_duration": 4.2, "Down/Up Ratio": 1.0,
        "Bwd IAT Min": 8.0, "Fwd IAT Min": 6.0, "Pkt Size Avg": 5.2,
        "total_forward_packets": 2, "Idle Mean": 0
    },
    "DDOS_LOIC_HTTP": {
        "flow_packets_per_sec": 14.5, "flow_duration": 18.0, "Down/Up Ratio": 0.0,
        "Bwd IAT Min": 0.0, "Fwd IAT Min": 0.1, "Pkt Size Avg": 6.2,
        "total_forward_packets": 250, "Idle Mean": 2.0
    },
    "SQL_INJECTION": {
        "flow_packets_per_sec": 10.2, "flow_duration": 5.0, "Down/Up Ratio": 0.8,
        "Bwd IAT Min": 1.8, "Fwd IAT Min": 1.5, "Pkt Size Avg": 7.0,
        "total_forward_packets": 12, "Idle Mean": 0
    },
    "DOS_SLOWLORIS": {
        "flow_packets_per_sec": 4.5, "flow_duration": 45.0, "Down/Up Ratio": 0.2,
        "Bwd IAT Min": 2.0, "Fwd IAT Min": 12.0, "Pkt Size Avg": 2.1,
        "total_forward_packets": 4, "Idle Mean": 35.0
    },
    "ZERO_DAY_ANOMALY": {
        "flow_packets_per_sec": 85.0, "flow_duration": 95.0, "Down/Up Ratio": 8.5,
        "Bwd IAT Min": 0.01, "Fwd IAT Min": 0.01, "Pkt Size Avg": 14.0,
        "total_forward_packets": 800, "Idle Mean": 0
    }
};

function applyPresetToSliders(preset) {
    const vals = PRESET_SLIDER_DEFAULTS[preset];
    if (!vals) return;
    for (const [k, v] of Object.entries(vals)) {
        const slider = document.getElementById(`slider-${k}`);
        const valSpan = document.getElementById(`val-${k}`);
        if (slider) slider.value = v;
        if (valSpan) valSpan.innerText = v;
    }
}

async function runSimulation() {
    const activePresetBtn = document.querySelector(".btn-preset.active");
    const preset = activePresetBtn ? activePresetBtn.getAttribute("data-preset") : "BENIGN";

    const customFeats = {};
    KEY_SIMULATOR_FEATURES.forEach(f => {
        const slider = document.getElementById(`slider-${f.key}`);
        if (slider) {
            customFeats[f.key] = parseFloat(slider.value);
        }
    });

    try {
        const res = await fetch("/api/simulate", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ preset: preset, features: customFeats })
        });

        if (res.ok) {
            const result = await res.json();
            renderSimulationResults(result);
        }
    } catch (err) {
        console.error("Simulation error:", err);
    }
}

function renderSimulationResults(data) {
    const verdictBanner = document.getElementById("verdictBanner");
    const verdictClass = document.getElementById("verdictClass");
    const verdictRoute = document.getElementById("verdictRoute");
    const verdictConf = document.getElementById("verdictConfidence");

    const pred = data.classification.predicted_class;
    const conf = data.classification.confidence_percent;
    const isAnomaly = data.kan_cvae.is_anomaly;

    if (verdictClass) {
        if (isAnomaly) {
            verdictClass.innerText = "ZERO-DAY ATTACK DETECTED";
            verdictClass.style.color = "var(--accent-purple)";
        } else {
            verdictClass.innerText = pred;
            verdictClass.style.color = pred === "BENIGN" ? "var(--accent-green)" : "var(--accent-rose)";
        }
    }

    if (verdictBanner) {
        if (isAnomaly || pred !== "BENIGN") {
            verdictBanner.className = "verdict-banner danger";
        } else {
            verdictBanner.className = "verdict-banner";
        }
    }

    if (verdictRoute) {
        verdictRoute.innerText = `Route: ${data.classification.route} (${data.classification.gating_status})`;
    }
    if (verdictConf) {
        verdictConf.innerText = `${conf}%`;
    }

    // KAN-CVAE Reconstruction Loss Meter
    const loss = data.kan_cvae.reconstruction_loss;
    const threshold = data.kan_cvae.evt_threshold;
    const meterFill = document.getElementById("kanMeterFill");
    const kanLossValue = document.getElementById("kanLossValue");
    const evtVerdictText = document.getElementById("evtVerdictText");

    if (kanLossValue) {
        kanLossValue.innerText = `Recon Loss: ${loss.toFixed(6)}`;
    }

    // Gauge percentage (mapping 0 to 0.05 scale to 0-100%)
    const pct = Math.min(100, Math.max(3, (loss / 0.04) * 100));
    if (meterFill) {
        meterFill.style.width = `${pct}%`;
        if (isAnomaly) {
            meterFill.className = "meter-fill exceeded";
        } else {
            meterFill.className = "meter-fill";
        }
    }

    if (evtVerdictText) {
        if (isAnomaly) {
            evtVerdictText.innerHTML = `<strong style="color: var(--accent-rose);">&#9888; CRITICAL:</strong> Reconstruction loss <strong style="color: #fff;">${loss}</strong> exceeded EVT threshold <strong style="color: #fff;">${threshold}</strong>. Identified as unseen Zero-Day network anomaly!`;
        } else {
            evtVerdictText.innerHTML = `Status: Reconstruction error (<strong style="color: var(--accent-green);">${loss}</strong>) below EVT limit (<strong style="color: var(--accent-purple);">${threshold}</strong>). Profile conforms to normal manifold.`;
        }
    }

    // SHAP Attribution Bars
    const shapContainer = document.getElementById("shapBarsContainer");
    if (shapContainer && data.shap_breakdown) {
        const maxShap = Math.max(...data.shap_breakdown.map(b => Math.abs(b.shap_value)), 0.01);
        shapContainer.innerHTML = data.shap_breakdown.map(b => {
            const isPos = b.shap_value >= 0;
            const barWidth = Math.min(100, (Math.abs(b.shap_value) / maxShap) * 100);
            return `
                <div class="shap-bar-row">
                    <div class="shap-bar-name" title="${b.feature}">${b.feature}</div>
                    <div class="shap-bar-track">
                        <div class="shap-bar-fill ${isPos ? 'pos' : 'neg'}" style="width: ${barWidth}%;"></div>
                    </div>
                    <div class="shap-bar-val ${isPos ? 'pos' : 'neg'}">${isPos ? '+' : ''}${b.shap_value.toFixed(4)}</div>
                </div>
            `;
        }).join("");
    }
}
