# Python-Aggregation

# Overview
**TASK: Aggregated Material Data**  

This project implements a robust data aggregation pipeline that consolidates multiple master and reference datasets into a single, analytics-ready output.
The pipeline ingests material, plant, storage, supplier, and manufacturer data from separate Excel files, infers relationships using shared business keys, and produces a unified result table.

It is designed with a strong focus on data correctness and integrity:
- The pipeline follows a modular simple structure, making it easy to adapt, extend.
- Preserve a clear and safe business granularity
- Detects and fails on duplicate records
- Logs meaningful diagnostics for fast issue resolution
- The result is a deterministic, production-grade dataset that reflects a clear business grain and can be safely consumed by downstream reporting, analytics, or operational systems.

Key Assumptions

- Final Granularity  
The storage data defines the final business grain. Each output row represents one physical inventory record at
(MaterialReference, Plant, StorageLocation, StorageBin), and uniqueness at this grain is strictly enforced.

- Supplier Handling  
If a material has multiple suppliers, the one with the lowest SupplierID is chosen only to keep the output deterministic, and to ensure reproducible output, not to represent business preference.

- Missing Supplier Information  
Materials without an associated supplier are retained in the output with SupplierName left as NULL, as missing supplier data is valid and no rule was provided to exclude or infer it.


# Requirements
- Python 3.9 or higher
---

## How to Run the Pipeline

Clone Repository
```bash

git clone <repository-url>
cd <repository-folder>
```

Create a virtual environment (recommended)

```bash
python -m venv venv
source venv/bin/activate   # Linux / Mac
venv\Scripts\activate      # Windows
 ```
 
Install the dependencies
```bash
pip install -r requirements.txt
```
Run the program
```bash
python main.py
```
Structure:

```
material-data-aggregation/
│
├── data/
│   ├── materials.xlsx
│   ├── plants.xlsx
│   ├── storage.xlsx
│   ├── suppliers.xlsx
│   ├── supplier-names.xlsx
│   └── manufacturer-names.xlsx
│
├── output/
│   └── result.xlsx         
│
├── logs/
│   └── aggregation_*.log   
│
├── main.py
├── requirements.txt
└── README.md
