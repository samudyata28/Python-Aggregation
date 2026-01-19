# Python-Aggregation

# Overview
**TASK: Aggregated Material Data**  

This project implements a robust Excel-based data aggregation pipeline that consolidates multiple master and reference datasets into a single, analytics-ready output.
The pipeline ingests material, plant, storage, supplier, and manufacturer data from separate Excel files, infers relationships using shared business keys, and produces a unified result table.

It is designed with a strong focus on data correctness and integrity:
- Enforces schema and key consistency
- Detects and fails on duplicate records
- Logs meaningful diagnostics for fast issue resolution
- The result is a deterministic, production-grade dataset that reflects a clear business grain and can be safely consumed by downstream reporting, analytics, or operational systems.

Key Assumptions

- MaterialReference uniquely identifies a material.
- ManufacturerID and SupplierID are 1-to-1 with their respective name tables.
- All joins are left joins to preserve the data unless business rule is specified.
- Duplicate records at the final stage are detected and logged for error.

Instructions to run the program

Clone Repository
```bash

git clone <repository-url>
cd <repository-folder>
```
Requirements
- python version 3.11 or higher
  
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
python-aggregation/
│
├── data/
│   ├── materials.xlsx
│   ├── plants.xlsx
│   ├── storage.xlsx
│   ├── suppliers.xlsx
│   ├── supplier-names.xlsx
│   ├── manufacturer-names.xlsx
│   └── result-template.xlsx
│
├── output/
│   └── result.xlsx
│
├── main.py
├── requirements.txt
└── README.md
