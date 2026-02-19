# The Dissipative Structure of Value

Information-geometric analysis of NYC real estate transactions (2015-2024) using Madelung Flow Regression.

**Paper:** [The Dissipative Structure of Value](The%20Dissipative%20Structure%20of%20Value%2C%20Kai%20Cobbs.pdf) (SSRN)

**Code:** `Information_Geometry_of_NYC_Sales.ipynb`

## Key Results
- 52-day optimal temporal bandwidth derived from Cramér-Rao bounds
- Exponential price certainty/velocity tradeoff (R²=0.81)
- 81% of dynamics are "housekeeping heat" 
- Excess heat Granger-causes Case-Shiller Index at 1-month lag (p=0.001)

## Methods
- Continuous-time kernel density estimation with causal constraints
- Fokker-Planck inversion for drift/diffusion extraction
- Information-geometric measures (Fisher information, Wasserstein geodesics via housekeeping reconstruction)
- Thermodynamic decomposition (Oono-Paniconi framework) 

Here are some html files created with plotly that have full 3d maneuverability and show different aspects of the Madelung Object aswell:

- Base 3D Visualization: https://github.com/HavensGuide/DissipativeValue/blob/main/market_evolution_4d(10YRS%2C%20REAL%20USD).html

- Trace of the Spatial Fisher Information Metric: https://github.com/HavensGuide/DissipativeValue/blob/main/metric_instability_field.html

- Information Kinetic Energy Density: https://github.com/HavensGuide/DissipativeValue/blob/main/kinetic_energy_density.html

- Where Each Borough Dominates: https://github.com/HavensGuide/DissipativeValue/blob/main/nyc_borough_territories.html

- Video of the Streamlines of the Information Gradient: https://github.com/HavensGuide/DissipativeValue/blob/main/market_flow_2d.mp4

**Contact:** kaicobbs4@gmail.com

