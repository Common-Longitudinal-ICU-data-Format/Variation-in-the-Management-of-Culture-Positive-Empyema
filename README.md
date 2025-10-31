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

## Execution Guide

### Step 1: Generate Empyema Cohort

This step identifies eligible hospitalizations, applies inclusion/exclusion criteria, and stratifies patients into treatment cohorts.

``` bash
uv run marimo edit code/01_cohort.py
```

### Step 2: Add Clinical Features and Generate Table 1

This step adds clinical features to the cohort and generates Table 1 summary statistics stratified by treatment group.

``` bash
uv run marimo edit code/02_table1.py
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
| **patient_procedures** | hospitalization_id, procedure_dttm, icd10_code, cpt_code | CPT Codes: 32035 (Thoracostomy with rib resection for empyema), 32036 (Thoracostomy with open flap drainage for empyema), 32100 (Thoracotomy with exploration), 32124 (Thoracotomy with open intrapleural pneumolysis), 32220 (Decortication pulmonary-total), 32225 (Decortication pulmonary partial), 32310 (Pleurectomy parietal), 32320 (Decortication and parietal pleurectomy), 32601 (Thoracoscopy diagnostic lungs and pleural space without biopsy), 32651 (Thoracoscopy surgical with partial pulmonary decortication), 32652 (Thoracoscopy surgical with total pulmonary decortication), 32653 (Thoracoscopy surgical with removal of intrapleural foreign body or fibrin deposit), 32656 (Thoracoscopy surgical with parietal pleurectomy), 32663 (Thoracoscopy surgical with lobectomy total or segmental), 32669 (Thoracoscopy with removal of single lung segment), 32670 (Thoracoscopy with removal of two lobes), 32671 (Thoracoscopy with removal of lung pneumonectomy), 32810 (Closure of chest wall following open flap drainage for empyema) |

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
- Elixhauser Comorbidity Index
- Charlson Comorbidity Index (CCI)
- Chronic pulmonary disease (from Elixhauser or CCI)

### Vital Signs
- `highest_temperature` (°C) - Maximum temperature during hospitalization
- `lowest_temperature` (°C) - Minimum temperature during hospitalization
- `lowest_map` (mmHg) - Minimum mean arterial pressure in ED, ward, or ICU

### Laboratory Values (Pre-Culture)
- `highest_wbc_before_culture` (10³/μL) - Maximum white blood cell count before pleural culture order
- `highest_creatinine_before_culture` (mg/dL) - Maximum creatinine before pleural culture order

### Interventions
- `vasopressor_ever` (binary) - Any vasopressor usage in ICU locations after culture order
- `NIPPV_ever` (binary) - Non-invasive positive pressure ventilation usage during hospitalization
- `HFNO_ever` (binary) - High-flow nasal cannula usage during hospitalization
- `IMV_ever` (binary) - Invasive mechanical ventilation usage during hospitalization
- `ICU_ever` (binary) - Any ICU admission during hospitalization (derived from ICU LOS > 0)

### Severity Scores
- `sofa_total` - Sequential Organ Failure Assessment score in first 24h from culture order (computed via clifpy ClifOrchestrator)

### Microbiology
- `organism_category` - Organisms from pleural cultures (combined for polymicrobial cultures)
- `organism_count` - Number of distinct organisms in pleural culture (1 = monomicrobial, >1 = polymicrobial)
- `culture_fungus` (binary) - Presence of fungal organisms (Candida, Aspergillus)

### Treatment Characteristics
- `received_intrapleural_lytic` (binary) - Received alteplase or dornase_alfa via intrapleural route
- `n_doses_alteplase` - Number of alteplase doses administered
- `n_doses_dornase_alfa` - Number of dornase_alfa doses administered
- `median_dose_alteplase` - Median dose of alteplase per patient (mg)
- `median_dose_dornase_alfa` - Median dose of dornase_alfa per patient (mg)
- `received_vats_decortication` (binary) - Received VATS or decortication procedure
- `treatment_group` - Assigned treatment group (antibiotics_only, intrapleural_lytics, vats_cohort)

### Antibiotic and Antifungal Exposure (ICU Only)
Binary flags for each medication administered in ICU locations after culture order:
- Antibiotics: cefepime, ceftriaxone, piperacillin_tazobactam, ampicillin_sulbactam, vancomycin, metronidazole, clindamycin, meropenem, imipenem, ertapenem, gentamicin, amikacin, levofloxacin, ciprofloxacin, amoxicillin_clavulanate
- Antifungals: fluconazole, micafungin, voriconazole, posaconazole, itraconazole

### Outcomes
- `icu_los_days` - ICU length of stay (days) - 0 if no ICU admission
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

### Step 4: Generate Table 1 Summary Statistics

Compute summary statistics for all variables stratified by cohort:
- Continuous variables: mean ± SD, median [IQR]
- Categorical variables: n (%)

### Step 5: Statistical Comparisons

Compare characteristics across three cohorts using:
- ANOVA or Kruskal-Wallis for continuous variables
- Chi-square or Fisher's exact test for categorical variables
- Calculate p-values for differences between cohorts

## Key Metrics Definitions

| Metric | Abbreviation | Definition |
|--------|--------------|------------|
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
- Elixhauser Comorbidity Index (mean, SD, median, IQR)
- Charlson Comorbidity Index (CCI) (mean, SD, median, IQR)
- Chronic Pulmonary Disease (n, %)

### Vital Signs
- Highest temperature (mean, SD, median, IQR)
- Lowest temperature (mean, SD, median, IQR)
- Lowest mean arterial pressure (MAP) in ED/ward/ICU (mean, SD, median, IQR)

### Severity
- SOFA score in first 24h from culture order (mean, SD, median, IQR)

### Interventions
- Vasopressor ever (ICU only) (n, %)
- NIPPV ever (n, %)
- HFNO ever (n, %)
- IMV ever (n, %)

### Laboratory Values (Pre-Culture)
- Highest WBC before culture (mean, SD, median, IQR)
- Highest creatinine before culture (mean, SD, median, IQR)

### Treatment-Specific (Intrapleural Lytics Cohort Only)
Among patients who received each lytic:
- Number of alteplase doses (mean, SD, median, IQR)
- Number of dornase_alfa doses (mean, SD, median, IQR)
- Median dose per patient of alteplase (mean, SD, median, IQR)
- Median dose per patient of dornase_alfa (mean, SD, median, IQR)

### Microbiology (Pleural Culture)
- Monomicrobial vs Polymicrobial (n, %)
- Fungal culture (n, %)

### Antibiotics and Antifungals (Ever Used in ICU)
Binary flags indicating if medication was administered in ICU locations after culture order:
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
- Amoxicillin-Clavulanate
- Fluconazole
- Micafungin
- Voriconazole
- Posaconazole
- Itraconazole

### Outcomes
- ICU ever (n, %)
- ICU length of stay days (mean, SD, median, IQR) - among those with ICU admission
- Hospital length of stay days (mean, SD, median, IQR)
- Inpatient mortality (n, %)

## Output Files

### PHI_DATA/ (Patient-Level - DO NOT SHARE)

These files contain patient-level data and should **NOT** be shared outside your institution:

- `cohort_empyema_initial.parquet` - Initial cohort after applying inclusion/exclusion criteria
- `cohort_filtering_stats.json` - Statistics showing filtering steps and counts
- `cohort_empyema_with_features.parquet` - Complete cohort with all clinical features and treatment group assignments

### upload_to_box/ (Safe to Share - Summary Statistics)

These files contain only aggregate summary statistics and are **safe to share** for multi-site comparison:

- `table1_descriptive_stats.csv` - Table 1 descriptive statistics stratified by treatment group (CSV format)
- `table1_statistics_by_treatment.json` - Table 1 statistics in JSON format (machine-readable)
- `organisms_by_treatment_group.csv` - Organism counts stratified by treatment group

## Data Sharing Instructions

### After Successful Pipeline Completion

Once both scripts (`01_cohort.py` and `02_table1.py`) have completed successfully and all output files are generated in `upload_to_box/`, contact the coordinating site to obtain the BOX folder link for uploading your results.

**Contact**: [Contact information to be provided]

### What to Upload

**IMPORTANT**: Upload **ONLY** files from the `upload_to_box/` folder. **DO NOT** upload files from `PHI_DATA/` folder as they contain patient-level data.

**Upload Checklist**:
- [ ] `table1_descriptive_stats.csv`
- [ ] `table1_statistics_by_treatment.json`
- [ ] `organisms_by_treatment_group.csv`

## Important Notes

### Procedure Code Identification

The following CPT codes are used to identify surgical intervention (VATS cohort):
- **CPT 32035**: Thoracostomy; with rib resection for empyema
- **CPT 32036**: Thoracostomy; with open flap drainage for empyema
- **CPT 32100**: Thoracotomy, with exploration
- **CPT 32124**: Thoracotomy with open intrapleural pneumolysis
- **CPT 32220**: Decortication, pulmonary-total
- **CPT 32225**: Decortication, pulmonary, partial
- **CPT 32310**: Pleurectomy, parietal
- **CPT 32320**: Decortication and parietal pleurectomy
- **CPT 32601**: Thoracoscopy, diagnostic lungs and pleural space, without biopsy
- **CPT 32651**: Thoracoscopy, surgical; with partial pulmonary decortication
- **CPT 32652**: Thoracoscopy, surgical; with total pulmonary decortication
- **CPT 32653**: Thoracoscopy, surgical; with removal of intrapleural foreign body or fibrin deposit
- **CPT 32656**: Thoracoscopy, surgical; with parietal pleurectomy
- **CPT 32663**: Thoracoscopy, surgical; with lobectomy, total or segmental
- **CPT 32669**: Thoracoscopy with removal of a single lung segment (segmentectomy)
- **CPT 32670**: Thoracoscopy with removal of two lobes (bilobectomy)
- **CPT 32671**: Thoracoscopy with removal of lung, pneumonectomy
- **CPT 32810**: Closure of chest wall following open flap drainage for empyema (Claggett type procedure)

These codes should be searched in both `hospital_diagnosis.icd10_code` (POA = 'no') and `patient_procedures` tables.

### Intrapleural Medication Administration

Intrapleural fibrinolytic medications must have `med_route_category == 'intrapleural'`:
- Alteplase (tissue plasminogen activator)
- Dornase alpha (recombinant human DNase)

### Antibiotic Treatment Window

The 5-day antibiotic treatment requirement begins at the **order_dttm** of the positive pleural culture, not the collection or result date.

### Multi-Site Comparisons

This project is designed for multi-site collaboration. Summary statistics from `upload_to_box/` will be aggregated across sites to:
- Characterize practice variation in empyema management
- Compare outcomes across treatment strategies
- Identify factors associated with treatment selection
- Evaluate temporal trends in management approaches
