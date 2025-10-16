# Variation in the Management of Culture-Positive Empyema

## Project Overview

Empyema is an infection of the pleural space. For patients with a positive culture in the pleural space, treatment approaches vary significantly across providers and medical centers. Management options include:
- **Antibiotic therapy alone**: Intravenous antibiotics to treat the infection
- **Intrapleural fibrinolytic therapy**: Medications (alteplase, dornase alpha) administered directly into the pleural space to break up the infection
- **Surgical intervention**: Video-assisted thoracoscopic surgery (VATS) or open thoracotomy with decortication to physically remove infected material

Despite established clinical guidelines, there is substantial heterogeneity in which approach, or combination of approaches, is selected for managing culture-positive empyema.

This multi-site project aims to characterize the variation in management strategies for culture-positive empyema across diverse US medical healthcare centers using the CLIF (Critical Care Learning from Intensive Care and Feeding) consortium data.

### Aim

To evaluate the heterogeneity in the management of culture-positive empyema care.

### Data Sources

**Database**: CLIF

**CLIF Sites**: Only sites with inpatient ward data (ICU stay not required)

**Study Period**: 2018-2024

## Setup

### 1. Install UV Package Manager

**Mac/Linux**:

``` bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

**Windows**:

``` powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

### 2. Install Dependencies

``` bash
uv sync
```

### 3. Configure Site

Create/update `clif_config.json` (rename _template.json) with your site-specific configuration:

``` json
{
    "site": "your_site_name",
    "data_directory": "/path/to/your/clif/data",
    "filetype": "parquet",
    "timezone": "US/Central"
}
```

## Required CLIF Tables

| Table | Columns | Categories/Filters |
|-------|---------|-------------------|
| **adt** | hospitalization_id, location_category, location_type, in_dttm, out_dttm | All location categories (ICU, ED, ward) |
| **crrt_therapy** | hospitalization_id, recorded_dttm, blood_flow | Filter: location_category in ('icu', 'ed') AND blood_flow > 0 |
| **hospital_diagnosis** | hospitalization_id, icd10_code, present_on_admission_category | POA = 'no' for procedure identification |
| **hospitalization** | patient_id, hospitalization_id, age_at_admission, admission_dttm, discharge_dttm, discharge_category | - |
| **labs** | hospitalization_id, lab_result_dttm, lab_category, lab_value, lab_value_numeric | wbc, creatinine |
| **medication_admin_intermittent** | hospitalization_id, admin_dttm, med_category, med_route_category, med_dose, med_dose_unit | CMS_sepsis_qualifying_antibiotics, alteplase, dornase_alpha (route = intrapleural) |
| **microbiology_culture** | hospitalization_id, order_dttm, collection_dttm, organism_category, fluid_category | fluid_category = 'pleural', organism_category != 'no growth' |
| **patient** | patient_id, sex_category, ethnicity_category, race_category, death_dttm | - |
| **respiratory_support** | hospitalization_id, recorded_dttm, device_category, fio2_set | NIPPV, High Flow NC, etc. |
| **patient_procedures** | hospitalization_id, procedure_dttm, icd10_code, cpt_code | Thoracoscopy and decortication procedures |

## Study Population

### Inclusion Criteria

- Adult patients aged ≥18 years
- Positive `organism_category` in `fluid_category == 'pleural'` (not "no growth")
- Received at least 5 days of intravenous antibiotics after the `order_dttm` of the positive pleural culture
  - Antibiotics: CMS_sepsis_qualifying_antibiotics from `medication_admin_intermittent`
- Admission date between January 1, 2018 and December 31, 2024

### Exclusion Criteria

- Hospitalization in the prior 6 weeks with a positive `organism_category` in `fluid_category == 'pleural'`
- Pleural organism was not treated (fewer than 5 days of IV antibiotics after `order_dttm` of positive culture)

### Censoring

- Analysis is grouped by `hospitalization_id` (not `patient_id`)
- Censoring occurs at the last row of hospitalization (discharge or death)

## Cohort Definitions

Patients meeting inclusion criteria are stratified into three mutually exclusive treatment cohorts based on management approach:

### 1. Antibiotic-Only Cohort

Patients treated with antibiotics alone without surgical or fibrinolytic intervention.

**Criteria**:
- Meets inclusion criteria and no exclusion criteria
- **NO** alteplase or dornase_alpha administered via intrapleural route
- **NO** ICD-10 codes in `hospital_diagnosis` (POA = 'no') or ICD-10/CPT codes in `patient_procedures` corresponding to:
  - Thoracoscopy, surgical; with partial pulmonary decortication (CPT 32651)
  - Thoracoscopy, surgical; with total pulmonary decortication (CPT 32652)
  - Decortication, pulmonary; partial (CPT 32225)
  - Decortication, pulmonary; total (CPT 32220)
  - Decortication and parietal pleurectomy (CPT 32320)

### 2. Intrapleural Lytics Cohort

Patients who received fibrinolytic therapy in addition to antibiotics.

**Criteria**:
- Meets inclusion criteria and no exclusion criteria
- Received alteplase OR dornase_alpha via intrapleural route (`med_route_category == 'intrapleural'`)
- **NEVER** received VATS or open thoracotomy with decortication (same procedure codes as above)

### 3. VATS Cohort

Patients who underwent surgical intervention.

**Criteria**:
- Meets inclusion criteria and no exclusion criteria
- Have ICD-10 diagnosis codes OR CPT procedure codes corresponding to VATS or open thoracotomy with decortication (see procedure codes above)

## Clinical Features Computed

All features are computed within the hospitalization window:

### Demographics
- Age at admission
- Sex
- Race/Ethnicity categories (Hispanic, Non-Hispanic White, Non-Hispanic Black, Non-Hispanic Asian, Other, Not Reported)
- BMI (first documented value during hospitalization)

### Comorbidities
- Charlson Comorbidity Index
- Charlson chronic pulmonary disease

### Vital Signs
- `highest_temperature` (°C) - Maximum temperature during hospitalization
- `lowest_temperature` (°C) - Minimum temperature during hospitalization
- `lowest_map` (mmHg) - Minimum mean arterial pressure in ED, ward, or ICU

### Laboratory Values (Pre-Culture)
- `highest_wbc_before_culture` (10³/μL) - Maximum white blood cell count before pleural culture order
- `highest_creatinine_before_culture` (mg/dL) - Maximum creatinine before pleural culture order

### Interventions
- `vasopressor_ever` (binary) - Any vasopressor usage during hospitalization
- `NIPPV_ever` (binary) - Non-invasive positive pressure ventilation usage
- `HFNO_ever` (binary) - High-flow nasal cannula usage
- `CVVH_ever` (binary) - Continuous venovenous hemofiltration in ICU or ED (non-missing blood_flow > 0)
- `ICU_ever` (binary) - Any ICU admission during hospitalization

### Severity Scores
- `highest_SOFA_score` - Maximum Sequential Organ Failure Assessment score (computed via clifpy ClifOrchestrator)

### Microbiology
- Top 10 most common organism categories from pleural cultures
- Co-positive cultures: Same organism in blood/buffy coat during hospitalization

### Treatment Characteristics (Intrapleural Lytics Cohort Only)
- Number of alteplase doses administered
- Number of dornase_alpha doses administered

### Outcomes
- `ICU_los_days` - ICU length of stay (days) - for patients with ICU admission
- `hospital_los_days` - Hospital length of stay (days)
- `inpatient_mortality` (binary) - Death during hospitalization

## Methodology

### Step 1: Identify Eligible Hospitalizations

1. Extract all hospitalizations with positive pleural cultures (2018-2024)
2. Apply inclusion criteria:
   - Age ≥18 years
   - Positive organism (not "no growth")
   - ≥5 days of IV antibiotics post-culture
3. Apply exclusion criteria:
   - Remove hospitalizations with pleural infection in prior 6 weeks
   - Remove hospitalizations with <5 days antibiotic treatment

### Step 2: Stratify into Treatment Cohorts

Classify each hospitalization into one of three mutually exclusive cohorts:

1. **Antibiotic-Only**: No lytics, no surgery
2. **Intrapleural Lytics**: Received lytics, no surgery
3. **VATS**: Received surgical intervention (regardless of lytics)

**Priority**: VATS > Intrapleural Lytics > Antibiotic-Only

### Step 3: Extract Clinical Features

For each hospitalization, compute:
- Demographics and comorbidities
- Vital signs (highest/lowest values)
- Laboratory values (pre-culture maximums)
- Intervention usage (binary flags)
- SOFA scores (using clifpy ClifOrchestrator)
- Microbiology results

### Step 4: Calculate Patient-Days (PD) per Cohort

Calculate total patient-days for each cohort using 24-hour windows from admission to discharge.

**Formula**: `PD = Σ (hospital_los_days for all hospitalizations in cohort)`

### Step 5: Calculate Days of Therapy (DOT) per Antibiotic

For each antibiotic listed in the Table 1 variables:

1. Count the number of 24-hour windows containing ≥1 dose of that antibiotic
2. If antibiotic is administered at any time during a window → count as **1 DOT**
3. Sum DOT across all hospitalizations in the cohort

**Formula**: `DOT = Number of windows with ≥1 dose of antibiotic`

### Step 6: Calculate DOT per 1000 PD per Cohort

For each antibiotic and each cohort:

**Formula**: `DOT per 1000 PD = (Total DOT for antibiotic / Total PD) × 1000`

**Example**:
- Antibiotic-Only Cohort: Total PD = 25,000
- Vancomycin DOT in Antibiotic-Only Cohort = 3,500
- Vancomycin DOT per 1000 PD = (3,500 / 25,000) × 1000 = **140**

### Step 7: Generate Table 1 Summary Statistics

Compute summary statistics for all variables stratified by cohort:
- Continuous variables: mean ± SD, median [IQR]
- Categorical variables: n (%)
- DOT per 1000 PD for specified antibiotics

### Step 8: Statistical Comparisons

Compare characteristics across three cohorts using:
- ANOVA or Kruskal-Wallis for continuous variables
- Chi-square or Fisher's exact test for categorical variables
- Calculate p-values for differences between cohorts

## Key Metrics Definitions

| Metric | Abbreviation | Definition |
|--------|--------------|------------|
| Days of Therapy | DOT | Number of 24-hour windows where patient receives ≥1 dose of antibiotic |
| Patient-Days | PD | Total number of 24-hour windows across all hospitalizations in a cohort |
| Intrapleural Fibrinolytic Therapy | - | Alteplase or dornase alpha administered via intrapleural route |
| VATS | - | Video-assisted thoracoscopic surgery with decortication |
| Sequential Organ Failure Assessment | SOFA | Organ dysfunction score (computed via clifpy ClifOrchestrator) |

## Table 1 Variables

The following variables will be summarized and stratified by treatment cohort:

### Demographics
- Age (median, IQR)
- Sex (n, %)
- Race/Ethnicity (n, %):
  - Hispanic
  - Non-Hispanic White
  - Non-Hispanic Black
  - Non-Hispanic Asian
  - Other
  - Not Reported
- BMI (first documented) (mean, SD)

### Comorbidities
- Charlson Comorbidity Index (mean, SD)
- Charlson Chronic Pulmonary Disease (n, %)

### Vital Signs
- Highest temperature (mean, SD)
- Lowest temperature (mean, SD)
- Lowest mean arterial pressure in ED/ward/ICU (mean, SD)

### Severity
- Highest SOFA score (mean, SD)

### Interventions
- Vasopressor ever (n, %)
- NIPPV ever (n, %)
- HFNO ever (n, %)
- CVVH in ICU or ED (n, %)

### Laboratory Values (Pre-Culture)
- Highest WBC before culture (mean, SD)
- Highest creatinine before culture (mean, SD)

### Treatment-Specific (Intrapleural Lytics Cohort Only)
- Number of alteplase doses (mean, SD)
- Number of dornase_alpha doses (mean, SD)

### Microbiology (Pleural Culture)
- 1st-10th most common organism_category (n, % of total)
- N, % patients with co-positive culture of same organism_category in blood/buffy coat during hospitalization

### Antibiotic Days of Therapy per 1000 PD
- Cefepime
- Ceftriaxone
- Piperacillin-Tazobactam
- Ampicillin-Sulbactam
- Vancomycin
- Metronidazole
- Clindamycin
- Meropenem
- Imipenem
- Ertapenem
- Gentamicin
- Amikacin
- Levofloxacin
- Ciprofloxacin

### Outcomes
- ICU ever (n, %)
- ICU length of stay days (mean, SD) - among those with ICU admission
- Hospital length of stay days (mean, SD)
- Inpatient mortality (n, %)

## Execution Guide

### Step 1: Generate Empyema Cohort

This step identifies eligible hospitalizations, applies inclusion/exclusion criteria, and stratifies patients into treatment cohorts.

**Mac/Linux (with logging)**:

``` bash
mkdir -p logs
uv run marimo run code/01_cohort.py 2>&1 | tee logs/01_cohort_output.log
```

**Windows PowerShell (with logging)**:

``` powershell
New-Item -ItemType Directory -Force -Path logs
uv run marimo run code/01_cohort.py 2>&1 | Tee-Object -FilePath logs/01_cohort_output.log
```

**Troubleshooting**: If you encounter errors, run in edit mode to see cell-level execution:

``` bash
uv run marimo edit code/01_cohort.py
```

### Step 2: Calculate DOT and Generate Table 1

This step computes DOT per 1000 PD for each antibiotic, calculates all clinical features, and generates Table 1 summary statistics.

**Mac/Linux (with logging)**:

``` bash
uv run marimo run code/02_DOT.py 2>&1 | tee logs/02_DOT_output.log
```

**Windows PowerShell (with logging)**:

``` powershell
uv run marimo run code/02_DOT.py 2>&1 | Tee-Object -FilePath logs/02_DOT_output.log
```

**Troubleshooting**:

``` bash
uv run marimo edit code/02_DOT.py
```

## Output Files

### PHI_DATA/ (Patient-Level - DO NOT SHARE)

These files contain patient-level data and should **NOT** be shared outside your institution:

- `cohort_empyema.parquet` - Complete cohort with all features, SOFA scores, and cohort assignments
- `dot_hospital_level.parquet` - DOT per antibiotic per hospitalization (wide format)
- `microbiology_cultures_pleural.parquet` - Pleural culture results per hospitalization
- `clinical_features_patient_level.parquet` - All computed clinical features per hospitalization

### RESULTS_UPLOAD_ME/ (Safe to Share - Summary Statistics)

These files contain only aggregate summary statistics and are **safe to share** for multi-site comparison:

- `table1_summary.json` - Complete Table 1 in JSON format (machine-readable)
- `table1_summary.csv` - Complete Table 1 in CSV format (human-readable)
- `cohort_summary.csv` - Summary statistics by treatment cohort
- `dot_by_cohort.csv` - DOT per 1000 PD by antibiotic and cohort
- `organism_distribution.csv` - Top organisms by cohort
- `outcomes_by_cohort.csv` - Outcomes stratified by treatment approach
- `statistical_comparisons.csv` - P-values for between-cohort comparisons

## Data Sharing Instructions

### After Successful Pipeline Completion

Once both scripts (`01_cohort.py` and `02_DOT.py`) have completed successfully and all output files are generated in `RESULTS_UPLOAD_ME/`, contact the coordinating site to obtain the BOX folder link for uploading your results.

**Contact**: [Contact information to be provided]

### What to Upload

**IMPORTANT**: Upload **ONLY** files from the `RESULTS_UPLOAD_ME/` folder. **DO NOT** upload files from `PHI_DATA/` folder as they contain patient-level data.

**Upload Checklist**:
- [ ] `table1_summary.json`
- [ ] `table1_summary.csv`
- [ ] `cohort_summary.csv`
- [ ] `dot_by_cohort.csv`
- [ ] `organism_distribution.csv`
- [ ] `outcomes_by_cohort.csv`
- [ ] `statistical_comparisons.csv`

## Important Notes

### Procedure Code Identification

The following CPT codes are used to identify surgical intervention (VATS cohort):
- **CPT 32651**: Thoracoscopy, surgical; with partial pulmonary decortication
- **CPT 32652**: Thoracoscopy, surgical; with total pulmonary decortication
- **CPT 32225**: Decortication, pulmonary; partial
- **CPT 32220**: Decortication, pulmonary; total
- **CPT 32320**: Decortication and parietal pleurectomy

These codes should be searched in both `hospital_diagnosis.icd10_code` (POA = 'no') and `patient_procedures` tables.

### Intrapleural Medication Administration

Intrapleural fibrinolytic medications must have `med_route_category == 'intrapleural'`:
- Alteplase (tissue plasminogen activator)
- Dornase alpha (recombinant human DNase)

### Antibiotic Treatment Window

The 5-day antibiotic treatment requirement begins at the **order_dttm** of the positive pleural culture, not the collection or result date.

### Multi-Site Comparisons

This project is designed for multi-site collaboration. Summary statistics from `RESULTS_UPLOAD_ME/` will be aggregated across sites to:
- Characterize practice variation in empyema management
- Compare outcomes across treatment strategies
- Identify factors associated with treatment selection
- Evaluate temporal trends in management approaches
