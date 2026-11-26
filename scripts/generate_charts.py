#!/usr/bin/env python3
"""
Generate publication-quality SVG charts for Big Data Final Report and Slides.
Pure Python standard library — zero third-party dependencies required.
"""
import os

DOCS_IMG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../docs/images"))
os.makedirs(DOCS_IMG_DIR, exist_ok=True)

# ----------------------------------------------------------------------
# 1. Chart 1: Storage Benchmark (CSV vs Parquet)
# ----------------------------------------------------------------------
def generate_chart1_storage():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 450" width="100%" height="100%" style="background:#ffffff; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
    <rect width="800" height="450" fill="#f8fafc" rx="12"/>
    <text x="400" y="45" text-anchor="middle" font-size="22" font-weight="bold" fill="#0f172a">Experiment 1: Storage Footprint &amp; Read I/O Benchmark</text>
    <text x="400" y="70" text-anchor="middle" font-size="14" fill="#64748b">10-Store Representative Subset (~10M rows) | CSV vs Parquet (Snappy)</text>

    <!-- Subplot 1: File Size (MB) -->
    <g transform="translate(60, 100)">
        <text x="140" y="30" text-anchor="middle" font-size="16" font-weight="bold" fill="#1e293b">Disk Footprint (MB)</text>
        <line x1="40" y1="260" x2="280" y2="260" stroke="#cbd5e1" stroke-width="2"/>
        
        <!-- Grid lines -->
        <line x1="40" y1="200" x2="280" y2="200" stroke="#f1f5f9" stroke-width="1"/>
        <text x="35" y="205" text-anchor="end" font-size="11" fill="#94a3b8">500</text>
        <line x1="40" y1="140" x2="280" y2="140" stroke="#f1f5f9" stroke-width="1"/>
        <text x="35" y="145" text-anchor="end" font-size="11" fill="#94a3b8">1000</text>
        <line x1="40" y1="80" x2="280" y2="80" stroke="#f1f5f9" stroke-width="1"/>
        <text x="35" y="85" text-anchor="end" font-size="11" fill="#94a3b8">1500</text>
        
        <!-- Bars -->
        <!-- CSV: 1,420 MB -> height: 1420 * 0.12 = 170.4 -> y: 260 - 170.4 = 89.6 -->
        <rect x="75" y="90" width="60" height="170" fill="#ef4444" rx="4"/>
        <text x="105" y="80" text-anchor="middle" font-size="13" font-weight="bold" fill="#b91c1c">1,420 MB</text>
        <text x="105" y="280" text-anchor="middle" font-size="13" font-weight="600" fill="#334155">Raw CSV</text>
        
        <!-- Parquet: 385 MB -> height: 385 * 0.12 = 46.2 -> y: 260 - 46.2 = 213.8 -->
        <rect x="185" y="214" width="60" height="46" fill="#10b981" rx="4"/>
        <text x="215" y="204" text-anchor="middle" font-size="13" font-weight="bold" fill="#047857">385 MB</text>
        <text x="215" y="280" text-anchor="middle" font-size="13" font-weight="600" fill="#334155">Parquet</text>

        <!-- Badge -->
        <rect x="160" y="120" width="110" height="28" rx="14" fill="#d1fae5"/>
        <text x="215" y="139" text-anchor="middle" font-size="12" font-weight="bold" fill="#065f46">↓ 72.9% Smaller</text>
    </g>

    <!-- Subplot 2: Read I/O Time (s) -->
    <g transform="translate(420, 100)">
        <text x="160" y="30" text-anchor="middle" font-size="16" font-weight="bold" fill="#1e293b">Read I/O Time (Seconds)</text>
        <line x1="40" y1="260" x2="300" y2="260" stroke="#cbd5e1" stroke-width="2"/>

        <!-- Grid lines -->
        <line x1="40" y1="200" x2="300" y2="200" stroke="#f1f5f9" stroke-width="1"/>
        <text x="35" y="205" text-anchor="end" font-size="11" fill="#94a3b8">100</text>
        <line x1="40" y1="140" x2="300" y2="140" stroke="#f1f5f9" stroke-width="1"/>
        <text x="35" y="145" text-anchor="end" font-size="11" fill="#94a3b8">200</text>
        <line x1="40" y1="80" x2="300" y2="80" stroke="#f1f5f9" stroke-width="1"/>
        <text x="35" y="85" text-anchor="end" font-size="11" fill="#94a3b8">300</text>

        <!-- CSV: 298.5 s -> height: 298.5 * 0.58 = 173.1 -> y = 260 - 173 = 87 -->
        <rect x="85" y="87" width="60" height="173" fill="#f97316" rx="4"/>
        <text x="115" y="77" text-anchor="middle" font-size="13" font-weight="bold" fill="#c2410c">298.5 s</text>
        <text x="115" y="280" text-anchor="middle" font-size="13" font-weight="600" fill="#334155">Raw CSV</text>

        <!-- Parquet: 74.0 s -> height: 74.0 * 0.58 = 42.9 -> y = 260 - 43 = 217 -->
        <rect x="195" y="217" width="60" height="43" fill="#3b82f6" rx="4"/>
        <text x="225" y="207" text-anchor="middle" font-size="13" font-weight="bold" fill="#1d4ed8">74.0 s</text>
        <text x="225" y="280" text-anchor="middle" font-size="13" font-weight="600" fill="#334155">Parquet</text>

        <!-- Badge -->
        <rect x="170" y="120" width="115" height="28" rx="14" fill="#dbeafe"/>
        <text x="227" y="139" text-anchor="middle" font-size="12" font-weight="bold" fill="#1e40af">⚡ 4.03× Faster</text>
    </g>

    <text x="400" y="420" text-anchor="middle" font-size="12" fill="#64748b">Key Finding: Columnar layout &amp; dictionary encoding drastically reduce disk space and unlock predicate pushdown.</text>
</svg>"""
    with open(os.path.join(DOCS_IMG_DIR, "chart1_storage_io.svg"), "w") as f:
        f.write(svg)
    print("✅ Created chart1_storage_io.svg")

# ----------------------------------------------------------------------
# 2. Chart 2: Model Accuracy vs Training Cost Trade-off
# ----------------------------------------------------------------------
def generate_chart2_models():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 450" width="100%" height="100%" style="background:#ffffff; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
    <rect width="800" height="450" fill="#f8fafc" rx="12"/>
    <text x="400" y="45" text-anchor="middle" font-size="22" font-weight="bold" fill="#0f172a">Experiment 2: Accuracy (NWRMSLE) vs. Training Time Trade-off</text>
    <text x="400" y="70" text-anchor="middle" font-size="14" fill="#64748b">Trained on 9.35M Rows (2 Workers, 4 Cores) | Evaluated on 299k Rows</text>

    <!-- Axes -->
    <g transform="translate(100, 90)">
        <line x1="0" y1="270" x2="620" y2="270" stroke="#94a3b8" stroke-width="2"/>
        <line x1="0" y1="0" x2="0" y2="270" stroke="#94a3b8" stroke-width="2"/>
        
        <!-- X Axis (Log-scale / Training Time categories) -->
        <text x="310" y="315" text-anchor="middle" font-size="14" font-weight="bold" fill="#334155">Training Time (Log Scale / Minutes)</text>
        
        <!-- Y Axis (NWRMSLE: 0.50 to 0.65) -->
        <text x="-135" y="-60" transform="rotate(-90)" text-anchor="middle" font-size="14" font-weight="bold" fill="#334155">NWRMSLE (Lower is Better ↓)</text>
        
        <!-- Grid lines and Y labels -->
        <!-- 0.65 -> y=0; 0.60 -> y=90; 0.55 -> y=180; 0.50 -> y=270 -->
        <line x1="0" y1="0" x2="620" y2="0" stroke="#e2e8f0" stroke-dasharray="4"/>
        <text x="-10" y="5" text-anchor="end" font-size="11" fill="#64748b">0.65</text>
        <line x1="0" y1="90" x2="620" y2="90" stroke="#e2e8f0" stroke-dasharray="4"/>
        <text x="-10" y="95" text-anchor="end" font-size="11" fill="#64748b">0.60</text>
        <line x1="0" y1="180" x2="620" y2="180" stroke="#e2e8f0" stroke-dasharray="4"/>
        <text x="-10" y="185" text-anchor="end" font-size="11" fill="#64748b">0.55</text>
        <line x1="0" y1="270" x2="620" y2="270" stroke="#e2e8f0" stroke-dasharray="4"/>
        <text x="-10" y="275" text-anchor="end" font-size="11" fill="#64748b">0.50</text>

        <!-- Points:
             Baseline: time=0 (x=50), nwrmsle=0.6301 (y: 270 - (0.6301-0.50)/0.15*270 = 270 - 234.18 = 35.8)
             LR: time=2.9m (x=210), nwrmsle=0.5481 (y: 270 - (0.5481-0.50)/0.15*270 = 270 - 86.58 = 183.4)
             RF: time=21.6m (x=380), nwrmsle=0.5229 (y: 270 - (0.5229-0.50)/0.15*270 = 270 - 41.22 = 228.8)
             GBT: time=186.8m (x=550), nwrmsle=0.5206 (y: 270 - (0.5206-0.50)/0.15*270 = 270 - 37.08 = 232.9)
        -->
        <!-- Connecting line -->
        <polyline points="50,36 210,183 380,229 550,233" fill="none" stroke="#6366f1" stroke-width="3" stroke-dasharray="6,4"/>

        <!-- Point 1: Baseline -->
        <circle cx="50" cy="36" r="8" fill="#64748b"/>
        <text x="50" y="22" text-anchor="middle" font-size="12" font-weight="bold" fill="#475569">Baseline (0 s)</text>
        <text x="50" y="55" text-anchor="middle" font-size="11" fill="#475569">NWRMSLE: 0.6301</text>

        <!-- Point 2: Linear Regression -->
        <circle cx="210" cy="183" r="8" fill="#3b82f6"/>
        <text x="210" y="165" text-anchor="middle" font-size="12" font-weight="bold" fill="#1d4ed8">Linear Reg (2.9 min)</text>
        <text x="210" y="202" text-anchor="middle" font-size="11" fill="#1e40af">NWRMSLE: 0.5481</text>

        <!-- Point 3: Random Forest (Sweet Spot!) -->
        <circle cx="380" cy="229" r="11" fill="#10b981" stroke="#059669" stroke-width="3"/>
        <text x="380" y="200" text-anchor="middle" font-size="13" font-weight="bold" fill="#047857">★ Random Forest (21.6 min)</text>
        <text x="380" y="252" text-anchor="middle" font-size="12" font-weight="bold" fill="#065f46">NWRMSLE: 0.5229</text>
        <rect x="325" y="145" width="115" height="24" rx="12" fill="#d1fae5"/>
        <text x="382" y="161" text-anchor="middle" font-size="11" font-weight="bold" fill="#065f46">Best Trade-off</text>

        <!-- Point 4: GBT -->
        <circle cx="550" cy="233" r="8" fill="#ef4444"/>
        <text x="550" y="215" text-anchor="middle" font-size="12" font-weight="bold" fill="#b91c1c">GBT (186.8 min / 3.1 h)</text>
        <text x="550" y="254" text-anchor="middle" font-size="11" fill="#991b1b">NWRMSLE: 0.5206</text>
        <rect x="495" y="265" width="115" height="24" rx="12" fill="#fee2e2"/>
        <text x="552" y="281" text-anchor="middle" font-size="11" font-weight="bold" fill="#991b1b">8.7× Slower for 0.4%</text>
    </g>

    <text x="400" y="425" text-anchor="middle" font-size="12" fill="#64748b">Conclusion: RF trains trees in parallel across nodes; GBT is sequential, causing extreme shuffle overhead on Spark.</text>
</svg>"""
    with open(os.path.join(DOCS_IMG_DIR, "chart2_model_tradeoff.svg"), "w") as f:
        f.write(svg)
    print("✅ Created chart2_model_tradeoff.svg")

# ----------------------------------------------------------------------
# 3. Chart 3: Data Scalability Benchmark (Exp 3)
# ----------------------------------------------------------------------
def generate_chart3_scalability():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 450" width="100%" height="100%" style="background:#ffffff; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
    <rect width="800" height="450" fill="#f8fafc" rx="12"/>
    <text x="400" y="45" text-anchor="middle" font-size="22" font-weight="bold" fill="#0f172a">Experiment 3: Data Scalability &amp; Stage Breakdown</text>
    <text x="400" y="70" text-anchor="middle" font-size="14" fill="#64748b">Fixed Cluster: 2 Workers (4 Cores, 2.4 GB RAM) | Random Forest Model</text>

    <!-- Legend -->
    <g transform="translate(180, 90)">
        <rect x="0" y="0" width="16" height="12" fill="#3b82f6" rx="2"/>
        <text x="24" y="11" font-size="12" fill="#334155">Parquet Read I/O</text>
        <rect x="150" y="0" width="16" height="12" fill="#8b5cf6" rx="2"/>
        <text x="174" y="11" font-size="12" fill="#334155">Feature Eng (Window Shuffle)</text>
        <rect x="360" y="0" width="16" height="12" fill="#10b981" rx="2"/>
        <text x="384" y="11" font-size="12" fill="#334155">RF Model Training</text>
    </g>

    <!-- Chart Body -->
    <g transform="translate(100, 120)">
        <line x1="40" y1="240" x2="580" y2="240" stroke="#94a3b8" stroke-width="2"/>
        <line x1="40" y1="0" x2="40" y2="240" stroke="#94a3b8" stroke-width="2"/>

        <!-- Y Axis ticks (0 to 1800 s, step 300) -->
        <text x="30" y="245" text-anchor="end" font-size="11" fill="#64748b">0</text>
        <line x1="35" y1="200" x2="580" y2="200" stroke="#f1f5f9" stroke-width="1"/>
        <text x="30" y="205" text-anchor="end" font-size="11" fill="#64748b">300s</text>
        <line x1="35" y1="160" x2="580" y2="160" stroke="#f1f5f9" stroke-width="1"/>
        <text x="30" y="165" text-anchor="end" font-size="11" fill="#64748b">600s</text>
        <line x1="35" y1="120" x2="580" y2="120" stroke="#f1f5f9" stroke-width="1"/>
        <text x="30" y="125" text-anchor="end" font-size="11" fill="#64748b">900s</text>
        <line x1="35" y1="80" x2="580" y2="80" stroke="#f1f5f9" stroke-width="1"/>
        <text x="30" y="85" text-anchor="end" font-size="11" fill="#64748b">1200s</text>
        <line x1="35" y1="40" x2="580" y2="40" stroke="#f1f5f9" stroke-width="1"/>
        <text x="30" y="45" text-anchor="end" font-size="11" fill="#64748b">1500s</text>

        <!-- Scale: 240 pixels = 1800 s -> 1 s = 0.1333 px -->
        <!-- Stacked Bar 1: 2M rows (Total: 330.3 s = 44 px) -->
        <!-- Read: 15.2s (2px), FE: 39.4s (5.3px), Train: 272.5s (36.3px) -->
        <g transform="translate(90, 0)">
            <rect x="0" y="238" width="70" height="2" fill="#3b82f6"/>
            <rect x="0" y="233" width="70" height="5" fill="#8b5cf6"/>
            <rect x="0" y="196" width="70" height="37" fill="#10b981" rx="4"/>
            <text x="35" y="185" text-anchor="middle" font-size="12" font-weight="bold" fill="#0f172a">330s (5.5m)</text>
            <text x="35" y="260" text-anchor="middle" font-size="13" font-weight="600" fill="#334155">2M Rows</text>
            <text x="35" y="278" text-anchor="middle" font-size="11" fill="#64748b">118 MB Shuffle</text>
        </g>

        <!-- Stacked Bar 2: 5M rows (Total: 803.6 s = 107.1 px) -->
        <!-- Read: 37.8s (5px), FE: 94.6s (12.6px), Train: 664.2s (88.5px) -->
        <g transform="translate(250, 0)">
            <rect x="0" y="235" width="70" height="5" fill="#3b82f6"/>
            <rect x="0" y="222" width="70" height="13" fill="#8b5cf6"/>
            <rect x="0" y="133" width="70" height="89" fill="#10b981" rx="4"/>
            <text x="35" y="122" text-anchor="middle" font-size="12" font-weight="bold" fill="#0f172a">804s (13.4m)</text>
            <text x="35" y="260" text-anchor="middle" font-size="13" font-weight="600" fill="#334155">5M Rows</text>
            <text x="35" y="278" text-anchor="middle" font-size="11" fill="#64748b">295 MB Shuffle</text>
        </g>

        <!-- Stacked Bar 3: 10M rows (Total: 1568.1 s = 209 px) -->
        <!-- Read: 74s (9.9px), FE: 185.2s (24.7px), Train: 1295.3s (172.7px) -->
        <g transform="translate(410, 0)">
            <rect x="0" y="230" width="70" height="10" fill="#3b82f6"/>
            <rect x="0" y="205" width="70" height="25" fill="#8b5cf6"/>
            <rect x="0" y="31" width="70" height="174" fill="#10b981" rx="4"/>
            <text x="35" y="20" text-anchor="middle" font-size="12" font-weight="bold" fill="#0f172a">1,568s (26.1m)</text>
            <text x="35" y="260" text-anchor="middle" font-size="13" font-weight="600" fill="#334155">10M Rows (Full)</text>
            <text x="35" y="278" text-anchor="middle" font-size="11" fill="#64748b">590 MB Shuffle</text>
        </g>

        <!-- Linear Trend Indicator Line -->
        <line x1="125" y1="196" x2="445" y2="31" stroke="#ef4444" stroke-width="2" stroke-dasharray="4"/>
    </g>

    <text x="400" y="425" text-anchor="middle" font-size="12" fill="#64748b">Key Finding: Near-linear complexity O(N). RF training accounts for 82.5% of pipeline time; shuffle volume scales with rows.</text>
</svg>"""
    with open(os.path.join(DOCS_IMG_DIR, "chart3_data_scalability.svg"), "w") as f:
        f.write(svg)
    print("✅ Created chart3_data_scalability.svg")

# ----------------------------------------------------------------------
# 4. Chart 4: Horizontal Resource Scaling & Speedup (Exp 4)
# ----------------------------------------------------------------------
def generate_chart4_scaling():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 800 450" width="100%" height="100%" style="background:#ffffff; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">
    <rect width="800" height="450" fill="#f8fafc" rx="12"/>
    <text x="400" y="45" text-anchor="middle" font-size="22" font-weight="bold" fill="#0f172a">Experiment 4: Horizontal Scaling &amp; Speedup vs. Amdahl's Law</text>
    <text x="400" y="70" text-anchor="middle" font-size="14" fill="#64748b">Fixed Data: 10M Rows (Full Subset) | 1 Worker (2C) vs 2 Workers (4C) vs 3 Workers (6C)</text>

    <!-- Subplot 1: Total Runtime Reduction -->
    <g transform="translate(60, 100)">
        <text x="140" y="25" text-anchor="middle" font-size="15" font-weight="bold" fill="#1e293b">Total Runtime (Minutes)</text>
        <line x1="40" y1="240" x2="280" y2="240" stroke="#cbd5e1" stroke-width="2"/>
        
        <!-- Y Axis Ticks (0 to 60 mins -> 240 px, 1 min = 4 px) -->
        <text x="35" y="245" text-anchor="end" font-size="11" fill="#94a3b8">0</text>
        <line x1="40" y1="160" x2="280" y2="160" stroke="#f1f5f9" stroke-width="1"/>
        <text x="35" y="165" text-anchor="end" font-size="11" fill="#94a3b8">20m</text>
        <line x1="40" y1="80" x2="280" y2="80" stroke="#f1f5f9" stroke-width="1"/>
        <text x="35" y="85" text-anchor="end" font-size="11" fill="#94a3b8">40m</text>

        <!-- 1 Worker: 49.8m -> height = 49.8 * 4 = 199.2 px -> y = 240 - 199 = 41 -->
        <rect x="65" y="41" width="50" height="199" fill="#ef4444" rx="4"/>
        <text x="90" y="32" text-anchor="middle" font-size="12" font-weight="bold" fill="#b91c1c">49.8 min</text>
        <text x="90" y="260" text-anchor="middle" font-size="12" font-weight="600" fill="#334155">1 Worker</text>
        <text x="90" y="275" text-anchor="middle" font-size="10" fill="#64748b">(2 Cores)</text>

        <!-- 2 Workers: 26.1m -> height = 26.1 * 4 = 104.4 px -> y = 240 - 104 = 136 -->
        <rect x="145" y="136" width="50" height="104" fill="#f59e0b" rx="4"/>
        <text x="170" y="127" text-anchor="middle" font-size="12" font-weight="bold" fill="#b45309">26.1 min</text>
        <text x="170" y="260" text-anchor="middle" font-size="12" font-weight="600" fill="#334155">2 Workers</text>
        <text x="170" y="275" text-anchor="middle" font-size="10" fill="#64748b">(4 Cores)</text>

        <!-- 3 Workers: 18.5m -> height = 18.5 * 4 = 74 px -> y = 240 - 74 = 166 -->
        <rect x="225" y="166" width="50" height="74" fill="#10b981" rx="4"/>
        <text x="250" y="157" text-anchor="middle" font-size="12" font-weight="bold" fill="#047857">18.5 min</text>
        <text x="250" y="260" text-anchor="middle" font-size="12" font-weight="600" fill="#334155">3 Workers</text>
        <text x="250" y="275" text-anchor="middle" font-size="10" fill="#64748b">(6 Cores)</text>
    </g>

    <!-- Subplot 2: Speedup vs Ideal Line -->
    <g transform="translate(420, 100)">
        <text x="160" y="25" text-anchor="middle" font-size="15" font-weight="bold" fill="#1e293b">Speedup Curve vs. Ideal Linear</text>
        <line x1="50" y1="240" x2="290" y2="240" stroke="#cbd5e1" stroke-width="2"/>
        <line x1="50" y1="40" x2="50" y2="240" stroke="#cbd5e1" stroke-width="2"/>

        <!-- Y Axis Ticks (0 to 3.5 -> 200 px, 1x = 57 px) -->
        <text x="42" y="245" text-anchor="end" font-size="11" fill="#94a3b8">0x</text>
        <line x1="50" y1="183" x2="290" y2="183" stroke="#f1f5f9" stroke-width="1"/>
        <text x="42" y="188" text-anchor="end" font-size="11" fill="#94a3b8">1.0x</text>
        <line x1="50" y1="126" x2="290" y2="126" stroke="#f1f5f9" stroke-width="1"/>
        <text x="42" y="131" text-anchor="end" font-size="11" fill="#94a3b8">2.0x</text>
        <line x1="50" y1="69" x2="290" y2="69" stroke="#f1f5f9" stroke-width="1"/>
        <text x="42" y="74" text-anchor="end" font-size="11" fill="#94a3b8">3.0x</text>

        <!-- X positions: 1W = 80, 2W = 170, 3W = 260 -->
        <!-- Ideal Linear: (80, 183), (170, 126), (260, 69) -->
        <line x1="80" y1="183" x2="260" y2="69" stroke="#94a3b8" stroke-width="2" stroke-dasharray="6,4"/>
        <text x="270" y="65" font-size="11" fill="#94a3b8">Ideal (3.0×)</text>

        <!-- Actual Measured Speedup: (80, 183 [1.0x]), (170, 131.7 [1.90x]), (260, 86.1 [2.70x]) -->
        <polyline points="80,183 170,132 260,86" fill="none" stroke="#2563eb" stroke-width="3"/>
        
        <circle cx="80" cy="183" r="6" fill="#2563eb"/>
        <text x="80" y="170" text-anchor="middle" font-size="12" font-weight="bold" fill="#1e40af">1.00×</text>
        <text x="80" y="260" text-anchor="middle" font-size="12" font-weight="600" fill="#334155">1 W</text>
        <text x="80" y="275" text-anchor="middle" font-size="10" fill="#047857">100% Eff</text>

        <circle cx="170" cy="132" r="6" fill="#2563eb"/>
        <text x="170" y="120" text-anchor="middle" font-size="12" font-weight="bold" fill="#1e40af">1.90×</text>
        <text x="170" y="260" text-anchor="middle" font-size="12" font-weight="600" fill="#334155">2 W</text>
        <text x="170" y="275" text-anchor="middle" font-size="10" fill="#047857">95.2% Eff</text>

        <circle cx="260" cy="86" r="6" fill="#2563eb"/>
        <text x="260" y="74" text-anchor="middle" font-size="12" font-weight="bold" fill="#1e40af">2.70×</text>
        <text x="260" y="260" text-anchor="middle" font-size="12" font-weight="600" fill="#334155">3 W</text>
        <text x="260" y="275" text-anchor="middle" font-size="10" fill="#047857">89.9% Eff</text>
    </g>

    <text x="400" y="425" text-anchor="middle" font-size="12" fill="#64748b">System Insight: 89.9% parallel efficiency at 3 workers. Network shuffle transfer &amp; DAG barrier synchronization impose slight sub-linear scaling.</text>
</svg>"""
    with open(os.path.join(DOCS_IMG_DIR, "chart4_resource_scaling.svg"), "w") as f:
        f.write(svg)
    print("✅ Created chart4_resource_scaling.svg")

if __name__ == "__main__":
    generate_chart1_storage()
    generate_chart2_models()
    generate_chart3_scalability()
    generate_chart4_scaling()
    print("🎉 All 4 charts successfully generated in docs/images/!")
