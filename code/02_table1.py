import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # Empyema Cohort: Table 1 - Add Clinical Features

    This notebook adds clinical features to the empyema cohort for Table 1 analysis.

    ## Features to Add

    **Demographics:**
    - sex_category
    - race_ethnicity (Hispanic, Non-Hispanic White, Non-Hispanic Black, Non-Hispanic Asian, Other, Not Reported)

    **Clinical Measurements:**
    - bmi (first recorded)
    - highest_temperature, lowest_temperature, lowest_map (vitals during stay)
    - vasopressor_ever (binary flag)
    - sofa_total (max during first 24h from 1st order)
    - NIPPV_ever, HFNO_ever, IMV_ever (respiratory support)

    **Outcomes:**
    - hospital_los_days
    - inpatient_mortality

    **Antibiotics (Ever Flags):**
    - cefepime, ceftriaxone, piperacillin_tazobactam, ampicillin_sulbactam
    - vancomycin, metronidazole, clindamycin
    - meropenem, imipenem, ertapenem
    - gentamicin, amikacin
    - levofloxacin, ciprofloxacin

    All time-based features filtered to stay window: 1st order_dttm → discharge_dttm
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Setup and Load Cohort""")
    return


@app.cell
def _():
    import pandas as pd
    import numpy as np
    import json
    from pathlib import Path
    from clifpy.tables import Patient, Vitals, MedicationAdminContinuous, MedicationAdminIntermittent, RespiratorySupport, Adt, Labs
    from clifpy.clif_orchestrator import ClifOrchestrator
    from clifpy.utils.outlier_handler import apply_outlier_handling
    import warnings
    warnings.filterwarnings('ignore')

    print("=== Empyema Cohort: Table 1 Feature Engineering ===")
    print("Setting up environment...")

    # Load site name from config
    with open('clif_config.json', 'r') as f:
        config = json.load(f)
    site_name = config.get('site', 'unknown')
    print(f"Site: {site_name}")
    return (
        Adt,
        ClifOrchestrator,
        Labs,
        MedicationAdminContinuous,
        MedicationAdminIntermittent,
        Path,
        Patient,
        RespiratorySupport,
        Vitals,
        apply_outlier_handling,
        pd,
        site_name,
    )


@app.cell
def _(Path, pd):
    # Load cohort from parquet
    print("\nLoading empyema cohort...")
    cohort_path = Path('PHI_DATA') / 'cohort_empyema_initial.parquet'
    cohort_base = pd.read_parquet(cohort_path)

    print(f"✓ Cohort loaded: {len(cohort_base):,} hospitalizations")
    print(f"  Columns: {list(cohort_base.columns)}")
    return (cohort_base,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Add ICU Length of Stay""")
    return


@app.cell
def _(Adt, cohort_base):
    # Load ADT data to calculate total ICU length of stay
    print("\nLoading ADT data for ICU LOS calculation...")

    cohort_hosp_ids_adt = cohort_base['hospitalization_id'].astype(str).unique().tolist()

    adt_table = Adt.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': cohort_hosp_ids_adt
        },
        columns=['hospitalization_id', 'location_category', 'in_dttm', 'out_dttm']
    )

    adt_df = adt_table.df.copy()
    print(f"✓ ADT data loaded: {len(adt_df):,} records")
    return (adt_df,)


@app.cell
def _(adt_df, pd):
    # Filter for ICU locations and calculate total ICU LOS per hospitalization
    print("\nCalculating total ICU length of stay...")

    # Normalize location_category to lowercase
    adt_df['location_category'] = adt_df['location_category'].str.lower()

    # Filter for ICU locations
    icu_adt = adt_df[adt_df['location_category'] == 'icu'].copy()
    print(f"  ICU location records: {len(icu_adt):,}")

    # Convert datetime columns
    icu_adt['in_dttm'] = pd.to_datetime(icu_adt['in_dttm'])
    icu_adt['out_dttm'] = pd.to_datetime(icu_adt['out_dttm'])

    # Calculate duration for each ICU stay in days
    icu_adt['icu_stay_duration_days'] = (icu_adt['out_dttm'] - icu_adt['in_dttm']).dt.total_seconds() / (24 * 3600)

    # Sum total ICU LOS per hospitalization
    icu_los_summary = icu_adt.groupby('hospitalization_id').agg({
        'icu_stay_duration_days': 'sum'
    }).reset_index()
    icu_los_summary.columns = ['hospitalization_id', 'icu_los_days']

    print(f"✓ ICU LOS calculated for {len(icu_los_summary):,} hospitalizations")
    print(f"  Mean ICU LOS: {icu_los_summary['icu_los_days'].mean():.2f} days")
    print(f"  Median ICU LOS: {icu_los_summary['icu_los_days'].median():.2f} days")
    return (icu_los_summary,)


@app.cell
def _(cohort_base, icu_los_summary, pd):
    # Merge ICU LOS to cohort
    print("\nMerging ICU LOS to cohort...")

    cohort_with_icu_los = pd.merge(
        cohort_base,
        icu_los_summary,
        on='hospitalization_id',
        how='left'
    )

    # Fill NaN (no ICU stay found) with 0
    cohort_with_icu_los['icu_los_days'] = cohort_with_icu_los['icu_los_days'].fillna(0)

    print(f"✓ ICU LOS merged: {len(cohort_with_icu_los):,} hospitalizations")
    print(f"  With ICU stay: {(cohort_with_icu_los['icu_los_days'] > 0).sum():,}")
    print(f"  Without ICU stay: {(cohort_with_icu_los['icu_los_days'] == 0).sum():,}")
    return (cohort_with_icu_los,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Add Demographics""")
    return


@app.cell
def _(Patient, cohort_with_icu_los, pd):
    # Load patient demographics
    print("\nLoading patient demographics...")

    patient_table = Patient.from_file(config_path='clif_config.json')
    patient_df = patient_table.df.copy()

    print(f"✓ Patient data loaded: {len(patient_df):,} records")

    # Merge demographics
    cohort_with_demo = pd.merge(
        cohort_with_icu_los,
        patient_df[['patient_id', 'sex_category', 'ethnicity_category', 'race_category']],
        on='patient_id',
        how='left'
    )

    # Create race_ethnicity column
    def categorize_race_ethnicity(row):
        ethnicity = str(row['ethnicity_category']).lower() if pd.notna(row['ethnicity_category']) else 'unknown'
        race = str(row['race_category']).lower() if pd.notna(row['race_category']) else 'unknown'

        # Check Non-Hispanic FIRST
        if 'non-hispanic' in ethnicity or 'not hispanic' in ethnicity:
            if 'white' in race:
                return 'Non-Hispanic White'
            elif 'black' in race or 'african american' in race:
                return 'Non-Hispanic Black'
            elif 'asian' in race:
                return 'Non-Hispanic Asian'
            else:
                return 'Other'

        # Check for Hispanic
        if 'hispanic' in ethnicity:
            return 'Hispanic'

        # If ethnicity is Other
        if ethnicity == 'other':
            return 'Other'

        # If ethnicity is Unknown or not reported
        if ethnicity in ['unknown', 'not reported', 'nan']:
            return 'Not Reported'

        return 'Other'

    cohort_with_demo['race_ethnicity'] = cohort_with_demo.apply(categorize_race_ethnicity, axis=1)

    print(f"✓ Demographics added")
    print(f"\nRace/Ethnicity distribution:")
    print(cohort_with_demo['race_ethnicity'].value_counts())
    return (cohort_with_demo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Calculate Hospital LOS and Mortality""")
    return


@app.cell
def _(cohort_with_demo):
    # Calculate hospital_los_days
    print("\nCalculating hospital LOS and mortality...")

    cohort_with_outcomes = cohort_with_demo.copy()

    # Hospital LOS in days
    cohort_with_outcomes['hospital_los_days'] = (
        cohort_with_outcomes['discharge_dttm'] - cohort_with_outcomes['admission_dttm']
    ).dt.total_seconds() / (24 * 3600)

    # Inpatient mortality
    cohort_with_outcomes['inpatient_mortality'] = cohort_with_outcomes['discharge_category'].fillna('').str.lower().apply(
        lambda x: 1 if any(term in x for term in ['expired', 'dead', 'death', 'deceased']) else 0
    )

    print(f"✓ Hospital LOS calculated")
    print(f"  Mean LOS: {cohort_with_outcomes['hospital_los_days'].mean():.2f} days")
    print(f"  Median LOS: {cohort_with_outcomes['hospital_los_days'].median():.2f} days")
    print(f"\n✓ Inpatient mortality calculated")
    print(f"  Deaths: {cohort_with_outcomes['inpatient_mortality'].sum():,} ({cohort_with_outcomes['inpatient_mortality'].mean()*100:.2f}%)")
    return (cohort_with_outcomes,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Add Vitals (Temperature, MAP) and BMI""")
    return


@app.cell
def _(Vitals, apply_outlier_handling, cohort_with_outcomes):
    # Load vitals data
    print("\nLoading vitals data...")

    cohort_hosp_ids = cohort_with_outcomes['hospitalization_id'].astype(str).unique().tolist()
    vital_categories = ['temp_c', 'map', 'height_cm', 'weight_kg']

    vitals_table = Vitals.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': cohort_hosp_ids,
            'vital_category': vital_categories
        },
        columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value']
    )

    print(f"✓ Vitals loaded: {len(vitals_table.df):,} records")

    # Apply outlier handling
    print("Applying outlier handling...")
    apply_outlier_handling(vitals_table)
    print(f"✓ Outlier handling applied: {len(vitals_table.df):,} records")

    vitals_df = vitals_table.df.copy()
    return (vitals_df,)


@app.cell
def _(cohort_with_outcomes, pd, vitals_df):
    # Filter vitals to stay window and calculate aggregates
    print("\nFiltering vitals to stay window (1st order → discharge)...")

    # Merge with cohort to get stay windows
    vitals_with_window = pd.merge(
        vitals_df,
        cohort_with_outcomes[['hospitalization_id', 'order_dttm', 'discharge_dttm']],
        on='hospitalization_id',
        how='inner'
    )

    # Filter to stay window
    vitals_stay = vitals_with_window[
        (vitals_with_window['recorded_dttm'] >= vitals_with_window['order_dttm']) &
        (vitals_with_window['recorded_dttm'] <= vitals_with_window['discharge_dttm'])
    ].copy()

    print(f"✓ Vitals filtered to stay window: {len(vitals_stay):,} records")

    # Separate temp/MAP from height/weight
    vitals_temp_map = vitals_stay[vitals_stay['vital_category'].isin(['temp_c', 'map'])].copy()
    vitals_bmi = vitals_stay[vitals_stay['vital_category'].isin(['height_cm', 'weight_kg'])].copy()

    # Calculate temp and MAP aggregates
    print("Calculating vital aggregates...")
    vitals_agg = vitals_temp_map.pivot_table(
        index='hospitalization_id',
        columns='vital_category',
        values='vital_value',
        aggfunc={'vital_value': ['min', 'max']}
    )

    vitals_agg.columns = ['_'.join(col).strip() for col in vitals_agg.columns.values]
    vitals_agg = vitals_agg.reset_index()

    # Rename columns
    vitals_rename = {
        'max_temp_c': 'highest_temperature',
        'min_temp_c': 'lowest_temperature',
        'min_map': 'lowest_map'
    }
    vitals_agg = vitals_agg.rename(columns={k: v for k, v in vitals_rename.items() if k in vitals_agg.columns})

    print(f"✓ Vital aggregates calculated for {len(vitals_agg):,} hospitalizations")

    # Calculate BMI from first recorded height/weight
    print("\nCalculating BMI from first recorded values...")
    vitals_bmi_sorted = vitals_bmi.sort_values('recorded_dttm')
    bmi_pivot = vitals_bmi_sorted.groupby(['hospitalization_id', 'vital_category'])['vital_value'].first().unstack()

    bmi_df = pd.DataFrame({
        'hospitalization_id': bmi_pivot.index,
        'height_cm': bmi_pivot.get('height_cm', pd.Series(dtype=float)),
        'weight_kg': bmi_pivot.get('weight_kg', pd.Series(dtype=float))
    }).reset_index(drop=True)

    bmi_df['bmi'] = bmi_df.apply(
        lambda row: row['weight_kg'] / ((row['height_cm'] / 100) ** 2)
        if pd.notna(row['height_cm']) and pd.notna(row['weight_kg']) and row['height_cm'] > 0
        else None,
        axis=1
    )

    print(f"✓ BMI calculated for {bmi_df['bmi'].notna().sum():,} hospitalizations")

    # Merge vitals and BMI
    vitals_complete = pd.merge(vitals_agg, bmi_df[['hospitalization_id', 'bmi']], on='hospitalization_id', how='outer')
    return (vitals_complete,)


@app.cell
def _(cohort_with_outcomes, pd, vitals_complete):
    # Merge vitals to cohort
    print("\nMerging vitals to cohort...")

    cohort_with_vitals = pd.merge(
        cohort_with_outcomes,
        vitals_complete,
        on='hospitalization_id',
        how='left'
    )

    print(f"✓ Vitals merged: {len(cohort_with_vitals):,} hospitalizations")
    return (cohort_with_vitals,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Add Vasopressor Flag""")
    return


@app.cell
def _(MedicationAdminContinuous, cohort_with_vitals):
    # Load vasopressor medications
    print("\nLoading vasopressor data...")

    vasopressor_categories = [
        'norepinephrine', 'epinephrine', 'phenylephrine', 'angiotensin',
        'vasopressin', 'dopamine', 'dobutamine', 'milrinone', 'isoproterenol'
    ]

    vaso_hosp_ids = cohort_with_vitals['hospitalization_id'].astype(str).unique().tolist()

    vaso_table = MedicationAdminContinuous.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': vaso_hosp_ids,
            'med_category': vasopressor_categories
        },
        columns=['hospitalization_id', 'admin_dttm', 'med_category']
    )

    vaso_df = vaso_table.df.copy()
    print(f"✓ Vasopressor data loaded: {len(vaso_df):,} records")
    return (vaso_df,)


@app.cell
def _(cohort_with_vitals, pd, vaso_df):
    # Filter vasopressors to stay window
    print("\nFiltering vasopressors to stay window...")

    vaso_with_window = pd.merge(
        vaso_df,
        cohort_with_vitals[['hospitalization_id', 'order_dttm', 'discharge_dttm']],
        on='hospitalization_id',
        how='inner'
    )

    vaso_stay = vaso_with_window[
        (vaso_with_window['admin_dttm'] >= vaso_with_window['order_dttm']) &
        (vaso_with_window['admin_dttm'] <= vaso_with_window['discharge_dttm'])
    ].copy()

    print(f"✓ Vasopressors filtered to stay: {len(vaso_stay):,} records")

    # Create binary flag
    vaso_summary = vaso_stay.groupby('hospitalization_id').size().reset_index()
    vaso_summary.columns = ['hospitalization_id', 'vaso_count']
    vaso_summary['vasopressor_ever'] = 1

    print(f"✓ Vasopressor flag created for {len(vaso_summary):,} hospitalizations")
    return (vaso_summary,)


@app.cell
def _(cohort_with_vitals, pd, vaso_summary):
    # Merge vasopressor flag
    cohort_with_vaso = pd.merge(
        cohort_with_vitals,
        vaso_summary[['hospitalization_id', 'vasopressor_ever']],
        on='hospitalization_id',
        how='left'
    )

    cohort_with_vaso['vasopressor_ever'] = cohort_with_vaso['vasopressor_ever'].fillna(0).astype(int)

    print(f"\n✓ Vasopressor flag merged")
    print(f"  With vasopressors: {(cohort_with_vaso['vasopressor_ever'] == 1).sum():,} ({(cohort_with_vaso['vasopressor_ever'] == 1).mean()*100:.1f}%)")
    return (cohort_with_vaso,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Add Respiratory Support Flags""")
    return


@app.cell
def _(RespiratorySupport, cohort_with_vaso):
    # Load respiratory support data
    print("\nLoading respiratory support data...")

    resp_hosp_ids = cohort_with_vaso['hospitalization_id'].astype(str).unique().tolist()

    resp_table = RespiratorySupport.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': resp_hosp_ids
        },
        columns=['hospitalization_id', 'recorded_dttm', 'device_category']
    )

    resp_df = resp_table.df.copy()
    print(f"✓ Respiratory support loaded: {len(resp_df):,} records")
    return (resp_df,)


@app.cell
def _(cohort_with_vaso, pd, resp_df):
    # Filter respiratory support to stay window
    print("\nFiltering respiratory support to stay window...")

    resp_with_window = pd.merge(
        resp_df,
        cohort_with_vaso[['hospitalization_id', 'order_dttm', 'discharge_dttm']],
        on='hospitalization_id',
        how='inner'
    )

    resp_stay = resp_with_window[
        (resp_with_window['recorded_dttm'] >= resp_with_window['order_dttm']) &
        (resp_with_window['recorded_dttm'] <= resp_with_window['discharge_dttm'])
    ].copy()

    print(f"✓ Respiratory support filtered to stay: {len(resp_stay):,} records")

    # Normalize device categories
    resp_stay['device_category_lower'] = resp_stay['device_category'].str.lower()

    # Create flags
    resp_summary = resp_stay.groupby('hospitalization_id').agg(
        NIPPV_ever=('device_category_lower', lambda x: 1 if any('nippv' in str(d) for d in x) else 0),
        HFNO_ever=('device_category_lower', lambda x: 1 if any('high flow nc' in str(d) for d in x) else 0),
        IMV_ever=('device_category_lower', lambda x: 1 if any('imv' in str(d) for d in x) else 0)
    ).reset_index()

    print(f"✓ Respiratory support flags created for {len(resp_summary):,} hospitalizations")
    return (resp_summary,)


@app.cell
def _(cohort_with_vaso, pd, resp_summary):
    # Merge respiratory support flags
    cohort_with_resp = pd.merge(
        cohort_with_vaso,
        resp_summary,
        on='hospitalization_id',
        how='left'
    )

    cohort_with_resp['NIPPV_ever'] = cohort_with_resp['NIPPV_ever'].fillna(0).astype(int)
    cohort_with_resp['HFNO_ever'] = cohort_with_resp['HFNO_ever'].fillna(0).astype(int)
    cohort_with_resp['IMV_ever'] = cohort_with_resp['IMV_ever'].fillna(0).astype(int)

    print(f"\n✓ Respiratory support flags merged")
    print(f"  NIPPV: {(cohort_with_resp['NIPPV_ever'] == 1).sum():,} ({(cohort_with_resp['NIPPV_ever'] == 1).mean()*100:.1f}%)")
    print(f"  HFNO: {(cohort_with_resp['HFNO_ever'] == 1).sum():,} ({(cohort_with_resp['HFNO_ever'] == 1).mean()*100:.1f}%)")
    print(f"  IMV: {(cohort_with_resp['IMV_ever'] == 1).sum():,} ({(cohort_with_resp['IMV_ever'] == 1).mean()*100:.1f}%)")
    return (cohort_with_resp,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Compute SOFA Scores""")
    return


@app.cell
def _(ClifOrchestrator, cohort_with_resp, pd):
    # Initialize ClifOrchestrator for SOFA
    print("\n=== SOFA Score Computation ===")
    print("Initializing ClifOrchestrator...")

    co_sofa = ClifOrchestrator(config_path='clif_config.json')

    # Prepare SOFA cohort (first 24h from 1st order)
    print("Preparing SOFA cohort (first 24h from 1st order)...")

    sofa_cohort_df = pd.DataFrame({
        'hospitalization_id': cohort_with_resp['hospitalization_id'],
        'start_time': cohort_with_resp['order_dttm'],
        'end_time': pd.Series([
            min(order + pd.Timedelta(hours=24), discharge)
            for order, discharge in zip(
                cohort_with_resp['order_dttm'],
                cohort_with_resp['discharge_dttm']
            )
        ])
    })

    sofa_hosp_ids = sofa_cohort_df['hospitalization_id'].astype(str).unique().tolist()

    print(f"✓ ClifOrchestrator initialized")
    print(f"✓ SOFA cohort prepared: {len(sofa_cohort_df):,} hospitalizations")
    return co_sofa, sofa_cohort_df, sofa_hosp_ids


@app.cell
def _(co_sofa, sofa_hosp_ids):
    # Load all tables for SOFA computation
    print("\nLoading tables for SOFA computation...")

    # Load labs
    co_sofa.load_table(
        'labs',
        filters={
            'hospitalization_id': sofa_hosp_ids,
            'lab_category': ['creatinine', 'platelet_count', 'po2_arterial', 'bilirubin_total']
        },
        columns=['hospitalization_id', 'lab_result_dttm', 'lab_category', 'lab_value_numeric']
    )
    print(f"  ✓ Labs loaded: {len(co_sofa.labs.df):,} records")

    # Load vitals
    co_sofa.load_table(
        'vitals',
        filters={
            'hospitalization_id': sofa_hosp_ids,
            'vital_category': ['map', 'spo2', 'weight_kg', 'height_cm']
        },
        columns=['hospitalization_id', 'recorded_dttm', 'vital_category', 'vital_value']
    )
    print(f"  ✓ Vitals loaded: {len(co_sofa.vitals.df):,} records")

    # Load patient assessments
    co_sofa.load_table(
        'patient_assessments',
        filters={
            'hospitalization_id': sofa_hosp_ids,
            'assessment_category': ['gcs_total']
        },
        columns=['hospitalization_id', 'recorded_dttm', 'assessment_category', 'numerical_value']
    )
    print(f"  ✓ Patient assessments loaded: {len(co_sofa.patient_assessments.df):,} records")

    # Load medication_admin_continuous
    co_sofa.load_table(
        'medication_admin_continuous',
        filters={
            'hospitalization_id': sofa_hosp_ids,
            'med_category': ['norepinephrine', 'epinephrine', 'dopamine', 'dobutamine']
        }
    )
    print(f"  ✓ Medications loaded: {len(co_sofa.medication_admin_continuous.df):,} records")

    # Load respiratory support
    co_sofa.load_table(
        'respiratory_support',
        filters={
            'hospitalization_id': sofa_hosp_ids
        },
        columns=['hospitalization_id', 'recorded_dttm', 'device_category', 'fio2_set']
    )
    print(f"  ✓ Respiratory support loaded: {len(co_sofa.respiratory_support.df):,} records")

    print("✓ All SOFA tables loaded")
    return


@app.cell
def _(co_sofa):
    # Clean medication data - remove null dose rows
    print("\nCleaning medication data...")

    med_df = co_sofa.medication_admin_continuous.df.copy()
    initial_count = len(med_df)

    # Remove null dose rows
    med_df = med_df[med_df['med_dose'].notna()]
    med_df = med_df[med_df['med_dose_unit'].notna()]
    med_df = med_df[~med_df['med_dose_unit'].astype(str).str.lower().isin(['nan', 'none', ''])]

    # Update the table
    co_sofa.medication_admin_continuous.df = med_df

    print(f"✓ Removed null doses: {initial_count:,} → {len(med_df):,} records")
    return


@app.cell
def _(co_sofa):
    # Convert medication units for SOFA
    print("\nConverting medication units for SOFA...")

    co_sofa.convert_dose_units_for_continuous_meds(
        preferred_units={
            'norepinephrine': 'mcg/kg/min',
            'epinephrine': 'mcg/kg/min',
            'dopamine': 'mcg/kg/min',
            'dobutamine': 'mcg/kg/min'
        },
        override=True
    )

    print("✓ Medication units converted")
    return


@app.cell
def _(co_sofa):
    # Filter to keep only successful conversions
    print("\nFiltering medications to keep only successful conversions...")

    med_df_converted = co_sofa.medication_admin_continuous.df_converted.copy()
    converted_initial_count = len(med_df_converted)

    # Keep only rows with successful conversion status
    med_df_success = med_df_converted[med_df_converted['_convert_status'] == 'success'].copy()

    # Update the orchestrator's converted dataframe
    co_sofa.medication_admin_continuous.df_converted = med_df_success

    conversion_removed_count = converted_initial_count - len(med_df_success)
    print(f"✓ Filtered to successful conversions: {converted_initial_count:,} → {len(med_df_success):,} records")
    print(f"  Removed {conversion_removed_count:,} failed conversions ({conversion_removed_count/converted_initial_count*100:.1f}%)")
    return


@app.cell
def _(co_sofa, sofa_cohort_df):
    # Import SOFA categories and create wide dataset
    from clifpy.utils.sofa import REQUIRED_SOFA_CATEGORIES_BY_TABLE

    print("\nCreating wide dataset for SOFA...")

    # Create wide dataset with SOFA-specific categories and time windows
    co_sofa.create_wide_dataset(
        category_filters=REQUIRED_SOFA_CATEGORIES_BY_TABLE,
        cohort_df=sofa_cohort_df,
        return_dataframe=True
    )

    print(f"✓ Wide dataset created: {co_sofa.wide_df.shape}")
    return


@app.cell
def _(co_sofa):
    # Add missing medication columns for SOFA
    print("\nChecking for missing medication columns...")

    required_med_cols = [
        'norepinephrine_mcg_kg_min',
        'epinephrine_mcg_kg_min',
        'dopamine_mcg_kg_min',
        'dobutamine_mcg_kg_min'
    ]

    missing_cols = [col for col in required_med_cols if col not in co_sofa.wide_df.columns]

    if missing_cols:
        for col_m in missing_cols:
            co_sofa.wide_df[col_m] = None
            print(f"  Added missing column: {col_m}")
        print(f"✓ Added {len(missing_cols)} missing medication columns")
    else:
        print("✓ All medication columns present")
    return


@app.cell
def _(co_sofa):
    # Compute SOFA scores
    print("\nComputing SOFA scores...")

    sofa_scores = co_sofa.compute_sofa_scores(
        wide_df=co_sofa.wide_df,
        id_name='hospitalization_id',
        fill_na_scores_with_zero=True,
        remove_outliers=True,
        create_new_wide_df=False
    )

    print(f"✓ SOFA scores computed: {sofa_scores.shape}")
    print(f"  Mean SOFA: {sofa_scores['sofa_total'].mean():.2f}")
    print(f"  Median SOFA: {sofa_scores['sofa_total'].median():.2f}")
    return (sofa_scores,)


@app.cell
def _(cohort_with_resp, pd, sofa_scores):
    # Merge SOFA scores to cohort
    print("\nMerging SOFA scores to cohort...")

    cohort_with_sofa = pd.merge(
        cohort_with_resp,
        sofa_scores[['hospitalization_id', 'sofa_total']],
        on='hospitalization_id',
        how='left'
    )

    print(f"✓ SOFA merged: {len(cohort_with_sofa):,} hospitalizations")
    if 'sofa_total' in cohort_with_sofa.columns:
        print(f"  Mean SOFA: {cohort_with_sofa['sofa_total'].mean():.2f}")
        print(f"  Median SOFA: {cohort_with_sofa['sofa_total'].median():.2f}")
    return (cohort_with_sofa,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Add Labs Before Culture (WBC, Creatinine)""")
    return


@app.cell
def _(Labs, apply_outlier_handling, cohort_with_sofa):
    # Load labs data for WBC and creatinine before culture
    print("\n=== Adding Labs Before Culture ===")
    print("Loading labs data (WBC, Creatinine)...")

    lab_categories = ['wbc', 'creatinine']
    cohort_hosp_ids_labs = cohort_with_sofa['hospitalization_id'].astype(str).unique().tolist()

    labs_table = Labs.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': cohort_hosp_ids_labs,
            'lab_category': lab_categories
        },
        columns=['hospitalization_id', 'lab_result_dttm', 'lab_category', 'lab_value_numeric']
    )

    print(f"✓ Labs loaded: {len(labs_table.df):,} records")

    # Apply outlier handling
    print("Applying outlier handling to labs...")
    apply_outlier_handling(labs_table)
    print(f"✓ Outlier handling applied: {len(labs_table.df):,} records")

    labs_df = labs_table.df.copy()
    return (labs_df,)


@app.cell
def _(cohort_with_sofa, labs_df, pd):
    # Filter labs to before culture order and calculate max values
    print("\nFiltering labs to before culture order_dttm...")

    # Merge labs with cohort to get order_dttm
    labs_with_order = pd.merge(
        labs_df,
        cohort_with_sofa[['hospitalization_id', 'order_dttm']],
        on='hospitalization_id',
        how='inner'
    )

    # Convert datetime
    labs_with_order['lab_result_dttm'] = pd.to_datetime(labs_with_order['lab_result_dttm'])
    labs_with_order['order_dttm'] = pd.to_datetime(labs_with_order['order_dttm'])

    # Filter to labs BEFORE culture order
    labs_before_culture = labs_with_order[
        labs_with_order['lab_result_dttm'] < labs_with_order['order_dttm']
    ].copy()

    print(f"✓ Labs before culture: {len(labs_before_culture):,} records")

    # Calculate highest values per hospitalization
    print("Calculating highest lab values before culture...")

    labs_pivot = labs_before_culture.pivot_table(
        index='hospitalization_id',
        columns='lab_category',
        values='lab_value_numeric',
        aggfunc='max'
    ).reset_index()

    # Rename columns
    labs_column_mapping = {
        'wbc': 'highest_wbc_before_culture',
        'creatinine': 'highest_creatinine_before_culture'
    }
    labs_existing_mappings = {k: v for k, v in labs_column_mapping.items() if k in labs_pivot.columns}
    labs_pivot = labs_pivot.rename(columns=labs_existing_mappings)

    print(f"✓ Lab aggregates calculated for {len(labs_pivot):,} hospitalizations")
    return (labs_pivot,)


@app.cell
def _(cohort_with_sofa, labs_pivot, pd):
    # Merge labs before culture to cohort
    print("\nMerging labs before culture to cohort...")

    cohort_with_labs_before = pd.merge(
        cohort_with_sofa,
        labs_pivot,
        on='hospitalization_id',
        how='left'
    )

    print(f"✓ Labs before culture merged: {len(cohort_with_labs_before):,} hospitalizations")
    if 'highest_wbc_before_culture' in cohort_with_labs_before.columns:
        print(f"  WBC available: {cohort_with_labs_before['highest_wbc_before_culture'].notna().sum():,}")
        print(f"  Mean WBC: {cohort_with_labs_before['highest_wbc_before_culture'].mean():.2f}")
    if 'highest_creatinine_before_culture' in cohort_with_labs_before.columns:
        print(f"  Creatinine available: {cohort_with_labs_before['highest_creatinine_before_culture'].notna().sum():,}")
        print(f"  Mean Creatinine: {cohort_with_labs_before['highest_creatinine_before_culture'].mean():.2f}")
    return (cohort_with_labs_before,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Add Antibiotic Ever Flags""")
    return


@app.cell
def _(MedicationAdminIntermittent, cohort_with_labs_before):
    # Load antibiotic data
    print("\n=== Adding Antibiotic Ever Flags ===")
    print("Loading antibiotic data...")

    antibiotic_categories = [
        'cefepime', 'ceftriaxone', 'piperacillin_tazobactam', 'ampicillin_sulbactam',
        'vancomycin', 'metronidazole', 'clindamycin',
        'meropenem', 'imipenem', 'ertapenem',
        'gentamicin', 'amikacin',
        'levofloxacin', 'ciprofloxacin'
    ]

    abx_hosp_ids = cohort_with_labs_before['hospitalization_id'].astype(str).unique().tolist()

    abx_table = MedicationAdminIntermittent.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': abx_hosp_ids,
            'med_category': antibiotic_categories
        },
        columns=['hospitalization_id', 'admin_dttm', 'med_category']
    )

    abx_df = abx_table.df.copy()
    print(f"✓ Antibiotic data loaded: {len(abx_df):,} records")
    print(f"  Unique antibiotics: {abx_df['med_category'].nunique()}")
    return abx_df, antibiotic_categories


@app.cell
def _(abx_df, cohort_with_labs_before, pd):
    # Filter antibiotics to stay window
    print("\nFiltering antibiotics to stay window...")

    abx_with_window = pd.merge(
        abx_df,
        cohort_with_labs_before[['hospitalization_id', 'order_dttm', 'discharge_dttm']],
        on='hospitalization_id',
        how='inner'
    )

    abx_stay = abx_with_window[
        (abx_with_window['admin_dttm'] >= abx_with_window['order_dttm']) &
        (abx_with_window['admin_dttm'] <= abx_with_window['discharge_dttm'])
    ].copy()

    print(f"✓ Antibiotics filtered to stay: {len(abx_stay):,} records")
    return (abx_stay,)


@app.cell
def _(abx_stay, antibiotic_categories):
    # Create ever flags for each antibiotic
    print("\nCreating antibiotic ever flags...")

    # Get unique hospitalizations with each antibiotic
    abx_flags_list = []

    for abx_cat in antibiotic_categories:
        abx_subset = abx_stay[abx_stay['med_category'] == abx_cat]['hospitalization_id'].unique()
        abx_flags_list.append({
            'antibiotic': abx_cat,
            'hospitalizations': set(abx_subset)
        })

    print(f"✓ Antibiotic ever flags prepared for {len(antibiotic_categories)} antibiotics")
    return (abx_flags_list,)


@app.cell
def _(abx_flags_list, cohort_with_labs_before):
    # Add flags to cohort
    print("\nAdding antibiotic flags to cohort...")

    cohort_with_abx = cohort_with_labs_before.copy()

    for abx_info in abx_flags_list:
        abx_category_name = abx_info['antibiotic']
        abx_hosps = abx_info['hospitalizations']
        col_name = f"{abx_category_name}_ever"

        cohort_with_abx[col_name] = cohort_with_abx['hospitalization_id'].apply(
            lambda x: 1 if x in abx_hosps else 0
        )

        abx_count = cohort_with_abx[col_name].sum()
        abx_pct = abx_count / len(cohort_with_abx) * 100
        print(f"  {abx_category_name}: {abx_count:,} ({abx_pct:.1f}%)")

    print(f"\n✓ All antibiotic flags added")
    return (cohort_with_abx,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Create Treatment Group Stratification""")
    return


@app.cell
def _(cohort_with_abx):
    # Create treatment group column based on interventions
    print("\n=== Creating Treatment Group Stratification ===")

    def assign_treatment_group(row):
        """Assign treatment group based on hierarchy:
        1. VATS (received_vats_decortication == 1), regardless of lytics
        2. Intrapleural lytics (lytics == 1, VATS == 0)
        3. Antibiotics only (VATS == 0, lytics == 0)
        """
        if row['received_vats_decortication'] == 1:
            return 'vats_cohort'
        elif row['received_intrapleural_lytic'] == 1:
            return 'intrapleural_lytics'
        else:
            return 'antibiotics_only'

    cohort_stratified = cohort_with_abx.copy()
    cohort_stratified['treatment_group'] = cohort_stratified.apply(assign_treatment_group, axis=1)

    print(f"\n✓ Treatment groups assigned")
    print(f"  Antibiotics only: {(cohort_stratified['treatment_group'] == 'antibiotics_only').sum():,} ({(cohort_stratified['treatment_group'] == 'antibiotics_only').mean()*100:.1f}%)")
    print(f"  Intrapleural lytics: {(cohort_stratified['treatment_group'] == 'intrapleural_lytics').sum():,} ({(cohort_stratified['treatment_group'] == 'intrapleural_lytics').mean()*100:.1f}%)")
    print(f"  VATS cohort: {(cohort_stratified['treatment_group'] == 'vats_cohort').sum():,} ({(cohort_stratified['treatment_group'] == 'vats_cohort').mean()*100:.1f}%)")
    return (cohort_stratified,)


@app.cell
def _(cohort_stratified, pd, site_name):
    # Create organism analysis by treatment group
    print("\n=== Creating Organism Analysis by Treatment Group ===")

    organism_rows = []

    for treatment_group in ['antibiotics_only', 'intrapleural_lytics', 'vats_cohort']:
        # Filter to current treatment group
        group_data = cohort_stratified[cohort_stratified['treatment_group'] == treatment_group]

        # Parse organism_category and count each organism
        organism_counts = {}

        for organism_category in group_data['organism_category']:
            if pd.notna(organism_category):
                # Split by "; " to get individual organisms
                organisms = [org.strip() for org in str(organism_category).split(';')]

                for organism in organisms:
                    if organism:  # Skip empty strings
                        organism_counts[organism] = organism_counts.get(organism, 0) + 1

        # Add to rows
        for organism, count in organism_counts.items():
            organism_rows.append({
                'site_name': site_name,
                'treatment_group': treatment_group,
                'organism': organism,
                'count': count
            })

        print(f"  ✓ {treatment_group}: {len(organism_counts)} unique organisms")

    organism_df = pd.DataFrame(organism_rows)

    # Sort by treatment_group and count (descending)
    organism_df = organism_df.sort_values(['treatment_group', 'count'], ascending=[True, False])

    print(f"\n✓ Organism analysis complete: {len(organism_df)} rows")
    return (organism_df,)


@app.cell
def _():
    # Helper function to suppress counts < 5
    def suppress_count(count):
        """Suppress counts < 5 for privacy protection.
        Returns '<5' if count < 5, otherwise returns count as string.
        """
        if count < 5:
            return '<5'
        else:
            return str(int(count))

    def format_count_pct(count, total):
        """Format count with percentage, applying suppression to count only."""
        count_str = suppress_count(count)
        if count_str == '<5':
            # Still calculate percentage but show count as <5
            pct = (count / total * 100) if total > 0 else 0
            return f"<5 ({pct:.1f}%)"
        else:
            pct = (count / total * 100) if total > 0 else 0
            return f"{count_str} ({pct:.1f}%)"

    print("✓ Suppression helper functions created")
    return format_count_pct, suppress_count


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Generate Table 1 Descriptive Statistics""")
    return


@app.cell
def _(cohort_stratified, format_count_pct, suppress_count):
    # Generate Table 1 summary statistics stratified by treatment group
    print("\n=== Generating Table 1 Descriptive Statistics (Stratified) ===")

    def generate_stats_for_group(df, group_name):
        """Generate statistics for a specific treatment group."""
        stats = {}
        n_total = len(df)

        # N and demographics
        stats['N'] = suppress_count(n_total)
        stats['Unique Patients'] = suppress_count(df['patient_id'].nunique())
        stats['Age (mean ± SD)'] = f"{df['age_at_admission'].mean():.1f} ± {df['age_at_admission'].std():.1f}"
        stats['Age (median [IQR])'] = f"{df['age_at_admission'].median():.1f} [{df['age_at_admission'].quantile(0.25):.1f}, {df['age_at_admission'].quantile(0.75):.1f}]"

        # Sex
        if 'sex_category' in df.columns:
            for sex_val in df['sex_category'].unique():
                count = (df['sex_category'] == sex_val).sum()
                stats[f'Sex: {sex_val}'] = format_count_pct(count, n_total)

        # Race/Ethnicity
        if 'race_ethnicity' in df.columns:
            for race_val in df['race_ethnicity'].unique():
                count = (df['race_ethnicity'] == race_val).sum()
                stats[f'Race/Ethnicity: {race_val}'] = format_count_pct(count, n_total)

        # BMI
        if 'bmi' in df.columns:
            stats['BMI (mean ± SD)'] = f"{df['bmi'].mean():.1f} ± {df['bmi'].std():.1f}"
            stats['BMI (median [IQR])'] = f"{df['bmi'].median():.1f} [{df['bmi'].quantile(0.25):.1f}, {df['bmi'].quantile(0.75):.1f}]"
            missing_count = df['bmi'].isna().sum()
            stats['BMI missing'] = format_count_pct(missing_count, n_total)

        # Microbial type (monomicrobial vs polymicrobial)
        if 'organism_count' in df.columns:
            monomicrobial_count = (df['organism_count'] == 1).sum()
            polymicrobial_count = (df['organism_count'] > 1).sum()
            stats['Monomicrobial'] = format_count_pct(monomicrobial_count, n_total)
            stats['Polymicrobial'] = format_count_pct(polymicrobial_count, n_total)

        # Vitals
        for col, name in [('highest_temperature', 'Highest Temp (°C)'),
                           ('lowest_temperature', 'Lowest Temp (°C)'),
                           ('lowest_map', 'Lowest MAP (mmHg)')]:
            if col in df.columns:
                stats[f'{name} (mean ± SD)'] = f"{df[col].mean():.1f} ± {df[col].std():.1f}"
                stats[f'{name} (median [IQR])'] = f"{df[col].median():.1f} [{df[col].quantile(0.25):.1f}, {df[col].quantile(0.75):.1f}]"
                missing_count = df[col].isna().sum()
                stats[f'{name} missing'] = format_count_pct(missing_count, n_total)

        # SOFA
        if 'sofa_total' in df.columns:
            stats['SOFA (mean ± SD)'] = f"{df['sofa_total'].mean():.1f} ± {df['sofa_total'].std():.1f}"
            stats['SOFA (median [IQR])'] = f"{df['sofa_total'].median():.1f} [{df['sofa_total'].quantile(0.25):.1f}, {df['sofa_total'].quantile(0.75):.1f}]"
            missing_count = df['sofa_total'].isna().sum()
            stats['SOFA missing'] = format_count_pct(missing_count, n_total)

        # Pre-culture Labs
        if 'highest_wbc_before_culture' in df.columns:
            stats['Highest WBC before culture (mean ± SD)'] = f"{df['highest_wbc_before_culture'].mean():.1f} ± {df['highest_wbc_before_culture'].std():.1f}"
            stats['Highest WBC before culture (median [IQR])'] = f"{df['highest_wbc_before_culture'].median():.1f} [{df['highest_wbc_before_culture'].quantile(0.25):.1f}, {df['highest_wbc_before_culture'].quantile(0.75):.1f}]"
            missing_count = df['highest_wbc_before_culture'].isna().sum()
            stats['Highest WBC before culture missing'] = format_count_pct(missing_count, n_total)

        if 'highest_creatinine_before_culture' in df.columns:
            stats['Highest Creatinine before culture (mean ± SD)'] = f"{df['highest_creatinine_before_culture'].mean():.2f} ± {df['highest_creatinine_before_culture'].std():.2f}"
            stats['Highest Creatinine before culture (median [IQR])'] = f"{df['highest_creatinine_before_culture'].median():.2f} [{df['highest_creatinine_before_culture'].quantile(0.25):.2f}, {df['highest_creatinine_before_culture'].quantile(0.75):.2f}]"
            missing_count = df['highest_creatinine_before_culture'].isna().sum()
            stats['Highest Creatinine before culture missing'] = format_count_pct(missing_count, n_total)

        # Support devices
        for support_col, support_name in [('vasopressor_ever', 'Vasopressor'),
                                           ('NIPPV_ever', 'NIPPV'),
                                           ('HFNO_ever', 'HFNO'),
                                           ('IMV_ever', 'IMV')]:
            if support_col in df.columns:
                count = (df[support_col] == 1).sum()
                stats[support_name] = format_count_pct(count, n_total)

        # Outcomes
        if 'hospital_los_days' in df.columns:
            stats['Hospital LOS (mean ± SD)'] = f"{df['hospital_los_days'].mean():.1f} ± {df['hospital_los_days'].std():.1f}"
            stats['Hospital LOS (median [IQR])'] = f"{df['hospital_los_days'].median():.1f} [{df['hospital_los_days'].quantile(0.25):.1f}, {df['hospital_los_days'].quantile(0.75):.1f}]"

        if 'icu_los_days' in df.columns:
            # Only calculate for patients with ICU stay (icu_los_days > 0)
            icu_patients = df[df['icu_los_days'] > 0]
            if len(icu_patients) > 0:
                stats['ICU LOS (mean ± SD) [ICU patients only]'] = f"{icu_patients['icu_los_days'].mean():.1f} ± {icu_patients['icu_los_days'].std():.1f}"
                stats['ICU LOS (median [IQR]) [ICU patients only]'] = f"{icu_patients['icu_los_days'].median():.1f} [{icu_patients['icu_los_days'].quantile(0.25):.1f}, {icu_patients['icu_los_days'].quantile(0.75):.1f}]"
                stats['N with ICU stay'] = suppress_count(len(icu_patients))

        if 'inpatient_mortality' in df.columns:
            count = (df['inpatient_mortality'] == 1).sum()
            stats['Inpatient Mortality'] = format_count_pct(count, n_total)

        # Lytic Medication Statistics (only among recipients)
        if 'n_doses_alteplase' in df.columns:
            # Only calculate for patients who received alteplase (n_doses > 0)
            alteplase_recipients = df[df['n_doses_alteplase'] > 0]
            if len(alteplase_recipients) > 0:
                stats['Alteplase doses (mean ± SD) [recipients only]'] = f"{alteplase_recipients['n_doses_alteplase'].mean():.1f} ± {alteplase_recipients['n_doses_alteplase'].std():.1f}"
                stats['Alteplase doses (median [IQR]) [recipients only]'] = f"{alteplase_recipients['n_doses_alteplase'].median():.1f} [{alteplase_recipients['n_doses_alteplase'].quantile(0.25):.1f}, {alteplase_recipients['n_doses_alteplase'].quantile(0.75):.1f}]"
                stats['N received alteplase'] = suppress_count(len(alteplase_recipients))

        if 'n_doses_dornase_alfa' in df.columns:
            # Only calculate for patients who received dornase alfa (n_doses > 0)
            dornase_recipients = df[df['n_doses_dornase_alfa'] > 0]
            if len(dornase_recipients) > 0:
                stats['Dornase alfa doses (mean ± SD) [recipients only]'] = f"{dornase_recipients['n_doses_dornase_alfa'].mean():.1f} ± {dornase_recipients['n_doses_dornase_alfa'].std():.1f}"
                stats['Dornase alfa doses (median [IQR]) [recipients only]'] = f"{dornase_recipients['n_doses_dornase_alfa'].median():.1f} [{dornase_recipients['n_doses_dornase_alfa'].quantile(0.25):.1f}, {dornase_recipients['n_doses_dornase_alfa'].quantile(0.75):.1f}]"
                stats['N received dornase alfa'] = suppress_count(len(dornase_recipients))

        if 'median_dose_alteplase' in df.columns:
            # Only calculate for patients who received alteplase (median_dose > 0)
            alteplase_dose_recipients = df[df['median_dose_alteplase'] > 0]
            if len(alteplase_dose_recipients) > 0:
                stats['Median alteplase dose per patient (mean ± SD) [recipients only]'] = f"{alteplase_dose_recipients['median_dose_alteplase'].mean():.1f} ± {alteplase_dose_recipients['median_dose_alteplase'].std():.1f}"
                stats['Median alteplase dose per patient (median [IQR]) [recipients only]'] = f"{alteplase_dose_recipients['median_dose_alteplase'].median():.1f} [{alteplase_dose_recipients['median_dose_alteplase'].quantile(0.25):.1f}, {alteplase_dose_recipients['median_dose_alteplase'].quantile(0.75):.1f}]"

        if 'median_dose_dornase_alfa' in df.columns:
            # Only calculate for patients who received dornase alfa (median_dose > 0)
            dornase_dose_recipients = df[df['median_dose_dornase_alfa'] > 0]
            if len(dornase_dose_recipients) > 0:
                stats['Median dornase alfa dose per patient (mean ± SD) [recipients only]'] = f"{dornase_dose_recipients['median_dose_dornase_alfa'].mean():.1f} ± {dornase_dose_recipients['median_dose_dornase_alfa'].std():.1f}"
                stats['Median dornase alfa dose per patient (median [IQR]) [recipients only]'] = f"{dornase_dose_recipients['median_dose_dornase_alfa'].median():.1f} [{dornase_dose_recipients['median_dose_dornase_alfa'].quantile(0.25):.1f}, {dornase_dose_recipients['median_dose_dornase_alfa'].quantile(0.75):.1f}]"

        # Antibiotics
        for col in df.columns:
            if col.endswith('_ever') and col not in ['vasopressor_ever', 'NIPPV_ever', 'HFNO_ever', 'IMV_ever', 'received_intrapleural_lytic', 'received_vats_decortication']:
                abx_name = col.replace('_ever', '').replace('_', ' ').title()
                count = (df[col] == 1).sum()
                stats[f'Antibiotic: {abx_name}'] = format_count_pct(count, n_total)

        return stats

    # Generate statistics for each treatment group
    table1_stratified = {}

    # Antibiotics only
    abx_only = cohort_stratified[cohort_stratified['treatment_group'] == 'antibiotics_only']
    table1_stratified['antibiotics_only'] = generate_stats_for_group(abx_only, 'Antibiotics Only')
    print(f"  ✓ Antibiotics only: {len(abx_only):,} patients")

    # Intrapleural lytics
    lytics = cohort_stratified[cohort_stratified['treatment_group'] == 'intrapleural_lytics']
    table1_stratified['intrapleural_lytics'] = generate_stats_for_group(lytics, 'Intrapleural Lytics')
    print(f"  ✓ Intrapleural lytics: {len(lytics):,} patients")

    # VATS cohort
    vats = cohort_stratified[cohort_stratified['treatment_group'] == 'vats_cohort']
    table1_stratified['vats_cohort'] = generate_stats_for_group(vats, 'VATS Cohort')
    print(f"  ✓ VATS cohort: {len(vats):,} patients")

    # Total
    table1_stratified['total'] = generate_stats_for_group(cohort_stratified, 'Total')
    print(f"  ✓ Total: {len(cohort_stratified):,} patients")

    print("✓ Stratified Table 1 statistics generated")
    return (table1_stratified,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Save Results""")
    return


@app.cell
def _(Path, cohort_stratified, organism_df, pd, site_name, table1_stratified):
    from datetime import datetime
    import json as _json

    print("\n=== Saving Results ===")

    # Create directories
    phi_dir = Path('PHI_DATA')
    phi_dir.mkdir(exist_ok=True)
    upload_dir = Path('upload_to_box')
    upload_dir.mkdir(exist_ok=True)

    # 1. Save row-level data to PHI_DATA
    cohort_output = phi_dir / 'cohort_empyema_with_features.parquet'
    cohort_stratified.to_parquet(cohort_output, index=False)
    print(f"✓ Enhanced cohort saved: {cohort_output}")
    print(f"  Rows: {len(cohort_stratified):,}")
    print(f"  Columns: {len(cohort_stratified.columns)}")

    # 2. Save organism analysis by treatment group
    print("\n✓ Saving organism analysis CSV...")
    organism_csv = upload_dir / 'organisms_by_treatment_group.csv'
    organism_df.to_csv(organism_csv, index=False)
    print(f"✓ Organism CSV saved: {organism_csv}")
    print(f"  Rows: {len(organism_df):,}")

    # 3. Create stratified CSV with site_name column
    print("\n✓ Creating stratified CSV output...")
    table1_rows = []

    # Get all unique variable names across all groups
    all_variables = set()
    for group_stats in table1_stratified.values():
        all_variables.update(group_stats.keys())

    # Create one row per variable with columns for each treatment group
    for variable in sorted(all_variables):
        row = {
            'site_name': site_name,
            'variable': variable,
            'antibiotics_only': table1_stratified['antibiotics_only'].get(variable, ''),
            'intrapleural_lytics': table1_stratified['intrapleural_lytics'].get(variable, ''),
            'vats_cohort': table1_stratified['vats_cohort'].get(variable, ''),
            'total': table1_stratified['total'].get(variable, '')
        }
        table1_rows.append(row)

    table1_csv = upload_dir / 'table1_descriptive_stats.csv'
    pd.DataFrame(table1_rows).to_csv(table1_csv, index=False)
    print(f"✓ Stratified CSV saved: {table1_csv}")

    # 4. Create JSON for multi-site aggregation
    print("\n✓ Creating JSON for multi-site aggregation...")
    json_output = {
        'site_name': site_name,
        'date_generated': datetime.now().isoformat(),
        'cohort_groups': table1_stratified
    }

    table1_json = upload_dir / 'table1_statistics_by_treatment.json'
    with open(table1_json, 'w') as _f:
        _json.dump(json_output, _f, indent=2)
    print(f"✓ JSON saved: {table1_json}")

    print(f"\n=== Outputs saved ===")
    print(f"  Row-level data: {phi_dir}/")
    print(f"  Summary files: {upload_dir}/")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
