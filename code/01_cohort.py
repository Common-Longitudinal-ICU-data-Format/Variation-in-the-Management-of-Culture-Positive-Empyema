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
    # Empyema Cohort Generation

    This notebook identifies hospitalizations with culture-positive empyema meeting inclusion criteria.

    ## Inclusion Criteria

    All of the following criteria must be met:

    1. **Adult patients aged >=18 years**
    2. **Positive organism_category in fluid_category == 'pleural'** (not "no growth")
    3. **Received at least 5 days of IV antibiotics** after order_dttm of positive pleural culture
       - Antibiotics: CMS_sepsis_qualifying_antibiotics from medication_admin_intermittent
       - Window: 5 consecutive days from culture order_dttm
    4. **Admission date between January 1, 2018 and December 31, 2024**
    5. **Discharge date <= December 31, 2024**

    ## Exclusion Criteria (to be implemented in next notebook)

    The following criteria will be applied in subsequent analysis:

    1. **Hospitalization in prior 6 weeks with positive pleural culture**
    2. **Fewer than 5 days of IV antibiotics** after order_dttm

    ## Additional Flags Tracked (not exclusion criteria)

    The following interventions are tracked but not used as exclusion criteria:

    1. **Intrapleural lytic therapy** (alteplase or dornase_alfa via intrapleural route)
       - Tracked from 1st culture order_dttm to discharge_dttm (entire stay)
       - Includes dose counts and median doses for alteplase and dornase_alfa
    2. **VATS/Decortication procedures** (hospitalization-level, any time during stay)
       - CPT codes: 32035, 32036, 32100, 32124, 32220, 32225, 32310, 32320, 32601, 32651, 32652, 32653, 32656, 32663, 32669, 32670, 32671, 32810
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Setup and Configuration""")
    return


@app.cell
def _():
    import pandas as pd
    import numpy as np
    from clifpy.tables import Hospitalization, MicrobiologyCulture, MedicationAdminIntermittent, PatientProcedures
    import warnings
    warnings.filterwarnings('ignore')

    print("=== Empyema Cohort Generation ===")
    print("Setting up environment...")
    return (
        Hospitalization,
        MedicationAdminIntermittent,
        MicrobiologyCulture,
        PatientProcedures,
        pd,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Load Hospitalization Data""")
    return


@app.cell
def _(Hospitalization, pd):
    # Load hospitalization data
    print("Loading hospitalization data...")

    hosp_table = Hospitalization.from_file(config_path='clif_config.json')
    hosp_df = hosp_table.df.copy()

    print(f"OK Hospitalization data loaded: {len(hosp_df):,} records")

    # Convert datetime columns
    hosp_df['admission_dttm'] = pd.to_datetime(hosp_df['admission_dttm'])
    hosp_df['discharge_dttm'] = pd.to_datetime(hosp_df['discharge_dttm'])

    # Check for null datetime values
    null_admission = hosp_df['admission_dttm'].isna().sum()
    null_discharge = hosp_df['discharge_dttm'].isna().sum()
    hosp_df = hosp_df[hosp_df['admission_dttm'].notna() & hosp_df['discharge_dttm'].notna()].copy()
    print(f"\nNull datetime check - Hospitalization: {null_admission:,} records with null admission_dttm, {null_discharge:,} records with null discharge_dttm removed")

    # Apply filters
    print("\nApplying hospitalization filters...")
    print(f"  Initial records: {len(hosp_df):,}")

    hosp_filtered = hosp_df[
        (hosp_df['age_at_admission'] >= 18) &
        (hosp_df['age_at_admission'].notna()) &
        (hosp_df['admission_dttm'].dt.year >= 2018) &
        (hosp_df['admission_dttm'].dt.year <= 2024) &
        (hosp_df['discharge_dttm'].dt.year <= 2024)
    ].copy()

    print(f"  After age filter (>=18) & date filters(2018-2024 admission, <=2024 discharge): {len(hosp_filtered):,}")
    return hosp_df, hosp_filtered


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Load Microbiology Culture Data (Pleural)""")
    return


@app.cell
def _(MicrobiologyCulture, hosp_filtered):
    import json

    # Load microbiology culture data for eligible hospitalizations
    print("\nLoading microbiology culture data...")

    # Load config to check site name
    with open('clif_config.json', 'r') as f:
        config = json.load(f)
    site_name = config.get('site', '').lower()

    # Get list of eligible hospitalization IDs
    eligible_hosp_ids = hosp_filtered['hospitalization_id'].astype(str).unique().tolist()
    print(f"  Eligible hospitalization IDs: {len(eligible_hosp_ids):,}")

    # Load microbiology culture with filters
    micro_table = MicrobiologyCulture.from_file(
        config_path='clif_config.json'
    )

    micro_df = micro_table.df.copy()
    print(f"OK Microbiology culture data loaded: {len(micro_df):,} records")

    # Convert hospitalization_id format (Rush-specific fix for .0 suffix)
    if site_name == 'rush':
        print("\nConverting hospitalization_id format (Rush-specific)...")
        micro_df['hospitalization_id'] = micro_df['hospitalization_id'].astype(float).astype(int).astype(str)
        print(f"OK Hospitalization IDs converted to clean string format (removed .0 suffix)")


    # Filter to only eligible hospitalization IDs
    print(f"\nFiltering to eligible hospitalizations...")
    print(f"  Before filter: {len(micro_df):,} records")
    micro_df = micro_df[micro_df['hospitalization_id'].isin(eligible_hosp_ids)].copy()
    print(f"  After filter: {len(micro_df):,} records")

    # Check for null datetime values in order_dttm
    print(f"\nNull datetime check - Microbiology...")
    null_order_dttm = micro_df['order_dttm'].isna().sum()
    micro_df = micro_df[micro_df['order_dttm'].notna()].copy()
    print(f"  Microbiology: {null_order_dttm:,} records with null order_dttm removed")

    # Check organism_category data quality
    print(f"\nOrganism category data quality check...")
    null_orgs = micro_df['organism_category'].isna().sum()
    empty_orgs = (micro_df['organism_category'] == '').sum()
    print(f"  Null organism_category values: {null_orgs:,}")
    print(f"  Empty string organism_category values: {empty_orgs:,}")

    # Filter to pleural fluid only
    print("\nApplying pleural fluid filter...")
    print(f"  Before pleural filter: {len(micro_df):,}")

    # Filter to pleural (looking for 'pleural' in fluid_category)
    micro_pleural = micro_df[
        micro_df['fluid_category'].str.lower().str.contains('pleural', na=False)
    ].copy()

    print(f"  After pleural filter: {len(micro_pleural):,}")

    # Exclude 'no growth', nulls, and empty strings with detailed reporting
    print(f"\nExcluding 'no growth', null, and empty organisms...")
    print(f"  Before filter: {len(micro_pleural):,}")

    # Count what will be filtered
    null_count = micro_pleural['organism_category'].isna().sum()
    empty_count = (micro_pleural['organism_category'] == '').sum()

    # Create normalized column for checking "no growth" variants
    organism_normalized = micro_pleural['organism_category'].fillna('').str.lower().str.strip()
    no_growth_variants = ['no_growth', 'no growth', 'nogrowth']
    no_growth_count = organism_normalized.isin(no_growth_variants).sum()

    print(f"  Filtering breakdown:")
    print(f"    Null organism_category: {null_count:,}")
    print(f"    Empty organism_category: {empty_count:,}")
    print(f"    'no growth' variants: {no_growth_count:,}")

    # Apply robust filter: exclude nulls, empty strings, and "no growth" variants
    micro_positive = micro_pleural[
        (micro_pleural['organism_category'].notna()) &
        (micro_pleural['organism_category'] != '') &
        (~organism_normalized.isin(no_growth_variants))
    ].copy()

    print(f"  After filter: {len(micro_positive):,}")
    print(f"OK Positive pleural cultures: {len(micro_positive):,}")

    # Exclude hospitalizations with tuberculosis or mycobacterium
    print(f"\nExcluding hospitalizations with tuberculosis or mycobacterium...")
    print(f"  Before TB/Mycobacterium filter: {len(micro_positive):,} culture records")
    print(f"  Unique hospitalizations: {micro_positive['hospitalization_id'].nunique():,}")

    # Find hospitalizations with TB/Mycobacterium in any culture
    tb_myco_hosps = micro_positive[
        micro_positive['organism_category'].str.lower().str.contains('tuberculosis|mycobacterium', na=False)
    ]['hospitalization_id'].unique()

    print(f"  Hospitalizations with TB/Mycobacterium: {len(tb_myco_hosps):,}")

    # Exclude these hospitalizations
    micro_positive = micro_positive[
        ~micro_positive['hospitalization_id'].isin(tb_myco_hosps)
    ].copy()

    print(f"  After TB/Mycobacterium filter: {len(micro_positive):,} culture records")
    print(f"  Unique hospitalizations: {micro_positive['hospitalization_id'].nunique():,}")
    return (micro_positive,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Group Organisms by Culture Event""")
    return


@app.cell
def _(micro_positive):
    # Group by culture event to prevent duplicate rows from polymicrobial cultures
    # (same patient, hospitalization, order time, fluid -> multiple organisms)
    print("\nGrouping organisms by culture event...")
    print(f"  Before grouping: {len(micro_positive):,} organism records")

    micro_grouped = micro_positive.groupby(
        ['patient_id', 'hospitalization_id', 'order_dttm', 'fluid_category'],
        as_index=False
    ).agg({
        'organism_category': lambda x: '; '.join(sorted(x.unique()))
    })

    # Add organism count column
    micro_grouped['organism_count'] = micro_grouped['organism_category'].str.count(';') + 1

    print(f"  After grouping: {len(micro_grouped):,} culture events")

    print(f"\nPolymicrobial cultures:")
    _mono_count = (micro_grouped['organism_count'] == 1).sum()
    _poly_count = (micro_grouped['organism_count'] > 1).sum()
    print(f"  Monomicrobial (1 organism): {_mono_count:,} ({_mono_count/len(micro_grouped)*100:.1f}%)")
    print(f"  Polymicrobial (>1 organism): {_poly_count:,} ({_poly_count/len(micro_grouped)*100:.1f}%)")
    if _poly_count > 0:
        print(f"  Max organisms in single culture: {micro_grouped['organism_count'].max()}")

    print(f"OK Grouped to one row per culture event")
    return (micro_grouped,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Merge Hospitalization with Pleural Cultures""")
    return


@app.cell
def _(hosp_filtered, micro_grouped, pd):
    # Merge hospitalization with positive pleural cultures
    print("\nMerging hospitalization with positive pleural cultures...")

    cohort_with_cultures = pd.merge(
        hosp_filtered[['patient_id', 'hospitalization_id', 'age_at_admission',
                      'admission_dttm', 'discharge_dttm', 'discharge_category']],
        micro_grouped[['patient_id', 'hospitalization_id','order_dttm','fluid_category','organism_category','organism_count']],
        on=['hospitalization_id', 'patient_id'],
        how='inner'
    )

    print(f"OK Hospitalizations with positive pleural cultures: {len(cohort_with_cultures):,}")
    print(f"  Unique hospitalizations: {cohort_with_cultures['hospitalization_id'].nunique():,}")
    print(f"  Unique patients: {cohort_with_cultures['patient_id'].nunique():,}")
    return (cohort_with_cultures,)


@app.cell
def _(cohort_with_cultures):
    cohort_with_cultures
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Load Antibiotic Data""")
    return


@app.cell
def _(MedicationAdminIntermittent, cohort_with_cultures):
    # Load medication data for CMS sepsis qualifying antibiotics
    print("\nLoading antibiotic administration data...")

    # Get hospitalization IDs with positive pleural cultures
    cohort_hosp_ids = cohort_with_cultures['hospitalization_id'].astype(str).unique().tolist()
    print(f"  Loading antibiotics for {len(cohort_hosp_ids):,} hospitalizations")

    # Load medications filtered to CMS_sepsis_qualifying_antibiotics
    meds_table = MedicationAdminIntermittent.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': cohort_hosp_ids
        },
        columns=['hospitalization_id', 'admin_dttm', 'med_category','med_group','med_route_category']
    )

    meds_df = meds_table.df[meds_table.df['med_group'].str.lower() == 'cms_sepsis_qualifying_antibiotics'].copy()
    print(f"OK CMS sepsis-qualifying antibiotics loaded: {len(meds_df):,} records")

    # Check for null datetime values in admin_dttm
    print(f"\nNull datetime check - Medications...")
    null_admin_dttm = meds_df['admin_dttm'].isna().sum()
    meds_df = meds_df[meds_df['admin_dttm'].notna()].copy()
    print(f"  Medications: {null_admin_dttm:,} records with null admin_dttm removed")
    return cohort_hosp_ids, meds_df


@app.cell
def _(cohort_with_cultures, meds_df, pd):
    # Filter antibiotics to those given AFTER pleural culture order_dttm
    print("\nFiltering antibiotics to post-culture administration...")

    # For each hospitalization+order_dttm, get antibiotics administered after order
    # Merge to get order_dttm for each hospitalization
    abx_with_order = pd.merge(
        meds_df,
        cohort_with_cultures[['hospitalization_id', 'order_dttm']],
        on='hospitalization_id',
        how='inner'
    )

    # Filter to antibiotics given AFTER culture order (allow same day)
    abx_post_culture = abx_with_order[
        abx_with_order['admin_dttm'] >= abx_with_order['order_dttm']
    ].copy()

    print(f"OK Antibiotic administrations after culture order: {len(abx_post_culture):,}")
    print(f"  Unique culture orders: {abx_post_culture.groupby(['hospitalization_id', 'order_dttm']).ngroups:,}")
    return (abx_post_culture,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Calculate 5-Day Antibiotic Pattern""")
    return


@app.cell
def _(abx_post_culture):
    # Limit antibiotics to 5-day window from order_dttm
    print("\nFiltering antibiotics to 5-day window from culture order...")

    abx_5day_window = abx_post_culture[
        (abx_post_culture['admin_dttm'] - abx_post_culture['order_dttm']).dt.total_seconds() <= (5 * 24 * 3600)
    ].copy()

    print(f"OK Antibiotics in 5-day window: {len(abx_5day_window):,}")
    print(f"  Unique culture orders: {abx_5day_window.groupby(['hospitalization_id', 'order_dttm']).ngroups:,}")
    return (abx_5day_window,)


@app.cell
def _(abx_5day_window, pd):
    from tqdm import tqdm

    print("\nCalculating 5-day antibiotic pattern per culture order...")

    def calculate_5day_pattern(group):
        """Calculate binary indicators for each day in 5-day window."""
        days_from_order = (group['admin_dttm'] - group['order_dttm'].iloc[0]).dt.total_seconds() / (24 * 3600)

        result = {}
        for day in range(1, 6):
            # Check if any antibiotic dose was given on this day
            # Day 1: 0 to <24h, Day 2: 24 to <48h, etc.
            has_abx = ((days_from_order >= day-1) & (days_from_order < day)).any()
            result[f'day_{day}_abx'] = 1 if has_abx else 0

        # Check if all 5 days have antibiotics
        result['all_5_days_abx'] = 1 if sum(result.values()) == 5 else 0

        # Count antibiotic-free days
        result['abx_free_days'] = 5 - sum([result[f'day_{day}_abx'] for day in range(1, 6)])

        return pd.Series(result)

    # Use tqdm for progress tracking - manual loop to avoid reset_index conflicts
    patterns_list = []
    for (hosp_id, order_dt), group in tqdm(abx_5day_window.groupby(['hospitalization_id', 'order_dttm']), desc="Processing culture orders"):
        pattern_series = calculate_5day_pattern(group)
        pattern_dict = pattern_series.to_dict()
        pattern_dict['hospitalization_id'] = hosp_id
        pattern_dict['order_dttm'] = order_dt
        patterns_list.append(pattern_dict)

    # Handle empty results gracefully
    if len(patterns_list) == 0:
        # Create empty DataFrame with expected column schema
        abx_pattern = pd.DataFrame(columns=[
            'hospitalization_id', 'order_dttm',
            'day_1_abx', 'day_2_abx', 'day_3_abx', 'day_4_abx', 'day_5_abx',
            'all_5_days_abx', 'abx_free_days'
        ])
        print(f"\nWARNING: No culture orders found with antibiotics in 5-day window!")
        print(f"  This suggests an upstream data issue - check previous filtering steps.")
    else:
        abx_pattern = pd.DataFrame(patterns_list)

    print(f"\nOK Pattern calculated for {len(abx_pattern):,} culture orders")

    # Only print statistics if we have data
    if len(abx_pattern) > 0:
        print(f"  All 5 days covered: {(abx_pattern['all_5_days_abx'] == 1).sum():,} ({(abx_pattern['all_5_days_abx'] == 1).sum()/len(abx_pattern)*100:.1f}%)")
        print(f"  Missing 1+ days: {(abx_pattern['all_5_days_abx'] == 0).sum():,} ({(abx_pattern['all_5_days_abx'] == 0).sum()/len(abx_pattern)*100:.1f}%)")

        # Show distribution of antibiotic-free days
        print(f"\n  Antibiotic-free days distribution:")
        for _free_days in sorted(abx_pattern['abx_free_days'].unique()):
            _count = (abx_pattern['abx_free_days'] == _free_days).sum()
            _pct = _count / len(abx_pattern) * 100
            print(f"    {_free_days} days free: {_count:,} ({_pct:.1f}%)")
    return (abx_pattern,)


@app.cell
def _(mo):
    mo.md(r"""## Load Intrapleural Lytic Therapy""")
    return


@app.cell
def _(MedicationAdminIntermittent, cohort_hosp_ids):
    # Load medications with intrapleural route
    print("\nLoading intrapleural medication data...")

    intrapleural_table = MedicationAdminIntermittent.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': cohort_hosp_ids
        },
        columns=['hospitalization_id', 'admin_dttm', 'med_category', 'med_route_category', 'med_dose']
    )

    # Filter to intrapleural lytics (alteplase or dornase alfa)
    intrapleural_df = intrapleural_table.df[
        (intrapleural_table.df['med_route_category'] == 'intrapleural') &
        (intrapleural_table.df['med_category'].isin(['alteplase', 'dornase_alfa']))
    ].copy()

    print(f"OK Intrapleural lytics loaded: {len(intrapleural_df):,} records")
    print(f"  Alteplase: {(intrapleural_df['med_category'] == 'alteplase').sum():,}")
    print(f"  Dornase alfa: {(intrapleural_df['med_category'] == 'dornase_alfa').sum():,}")

    # Convert med_dose to numeric (handles string values at some sites)
    print(f"\nConverting med_dose to numeric...")
    null_before = intrapleural_df['med_dose'].isna().sum()
    intrapleural_df['med_dose'] = pd.to_numeric(intrapleural_df['med_dose'], errors='coerce')
    null_after = intrapleural_df['med_dose'].isna().sum()
    converted_to_null = null_after - null_before

    if converted_to_null > 0:
        print(f"  WARNING: {converted_to_null:,} records had non-numeric med_dose values (converted to NaN)")
        intrapleural_df = intrapleural_df[intrapleural_df['med_dose'].notna()].copy()
        print(f"  Removed {converted_to_null:,} records with invalid doses")
    print(f"  OK med_dose converted to numeric: {len(intrapleural_df):,} records remain")

    # Check for null datetime values in admin_dttm
    print(f"\nNull datetime check - Intrapleural medications...")
    null_intrapleural_admin = intrapleural_df['admin_dttm'].isna().sum()
    intrapleural_df = intrapleural_df[intrapleural_df['admin_dttm'].notna()].copy()
    print(f"  Intrapleural: {null_intrapleural_admin:,} records with null admin_dttm removed")
    return (intrapleural_df,)


@app.cell
def _(cohort_with_cultures, intrapleural_df, pd):
    # Merge with cohort to get first order_dttm and discharge_dttm per hospitalization
    print("\nPreparing hospitalization-level dates for lytics tracking...")

    # Get first order_dttm per hospitalization from cohort
    hosp_dates = cohort_with_cultures.groupby('hospitalization_id', as_index=False).agg({
        'order_dttm': 'min',  # First culture order
        'discharge_dttm': 'first'
    })

    intrapleural_with_dates = pd.merge(
        intrapleural_df,
        hosp_dates,
        on='hospitalization_id',
        how='inner'
    )

    # Filter to entire stay window: from first order_dttm to discharge_dttm
    print("\nFiltering intrapleural lytics to entire stay window (1st order -> discharge)...")
    intrapleural_stay = intrapleural_with_dates[
        (intrapleural_with_dates['admin_dttm'] >= intrapleural_with_dates['order_dttm']) &
        (intrapleural_with_dates['admin_dttm'] <= intrapleural_with_dates['discharge_dttm'])
    ].copy()

    print(f"OK Intrapleural lytics in entire stay window: {len(intrapleural_stay):,}")
    print(f"  Unique hospitalizations with lytics: {intrapleural_stay['hospitalization_id'].nunique():,}")
    print(f"  Alteplase: {(intrapleural_stay['med_category'] == 'alteplase').sum():,}")
    print(f"  Dornase alfa: {(intrapleural_stay['med_category'] == 'dornase_alfa').sum():,}")
    return (intrapleural_stay,)


@app.cell
def _(intrapleural_stay, pd):
    # Calculate hospitalization-level lytics statistics with dose counts and medians
    print("\nCalculating hospitalization-level lytic statistics...")

    def calc_lytic_stats(group):
        """Calculate dose counts and medians for alteplase and dornase_alfa."""
        alteplase_doses = group[group['med_category'] == 'alteplase']['med_dose']
        dornase_doses = group[group['med_category'] == 'dornase_alfa']['med_dose']

        return {
            'received_intrapleural_lytic': 1,
            'n_doses_alteplase': len(alteplase_doses),
            'n_doses_dornase_alfa': len(dornase_doses),
            'median_dose_alteplase': float(alteplase_doses.median()) if len(alteplase_doses) > 0 else 0.0,
            'median_dose_dornase_alfa': float(dornase_doses.median()) if len(dornase_doses) > 0 else 0.0
        }

    lytics_received = intrapleural_stay.groupby('hospitalization_id', as_index=False).apply(
        lambda g: pd.Series(calc_lytic_stats(g)),
        include_groups=False
    )

    print(f"OK Hospitalizations with intrapleural lytics: {len(lytics_received):,}")
    print(f"  With alteplase: {(lytics_received['n_doses_alteplase'] > 0).sum():,}")
    print(f"  With dornase alfa: {(lytics_received['n_doses_dornase_alfa'] > 0).sum():,}")
    print(f"  With both: {((lytics_received['n_doses_alteplase'] > 0) & (lytics_received['n_doses_dornase_alfa'] > 0)).sum():,}")
    return (lytics_received,)


@app.cell
def _(mo):
    mo.md(r"""## Load VATS/Decortication Procedures""")
    return


@app.cell
def _(PatientProcedures, cohort_hosp_ids):
    # Load patient procedures for cohort hospitalizations
    print("\nLoading patient procedure data...")

    proc_table = PatientProcedures.from_file(
        config_path='clif_config.json',
        filters={
            'hospitalization_id': cohort_hosp_ids
        },
        columns=['hospitalization_id', 'procedure_code', 'procedure_code_format']
    )

    # Filter to VATS/decortication CPT codes
    vats_cpt_codes = ['32035', '32036', '32100', '32124', '32220', '32225', '32310', '32320', '32601', '32651', '32652', '32653', '32656', '32663', '32669', '32670', '32671', '32810']
    proc_df = proc_table.df[
        (proc_table.df['procedure_code_format'].str.lower().str.contains('cpt', na=False)) &
        (proc_table.df['procedure_code'].isin(vats_cpt_codes))
    ].copy()

    print(f"OK VATS/Decortication procedures loaded: {len(proc_df):,} records")
    if len(proc_df) > 0:
        print(f"  CPT code distribution:")
        for _code in vats_cpt_codes:
            _code_count = (proc_df['procedure_code'] == _code).sum()
            if _code_count > 0:
                print(f"    {_code}: {_code_count:,}")

    # # Check for null datetime values in procedure_billed_dttm
    # print(f"\nNull datetime check - Procedures...")
    # null_procedure_dttm = proc_df['procedure_billed_dttm'].isna().sum()
    # proc_df = proc_df[proc_df['procedure_billed_dttm'].notna()].copy()
    # print(f"  Procedures: {null_procedure_dttm:,} records with null procedure_billed_dttm removed")
    return (proc_df,)


@app.cell
def _(proc_df):
    # Create hospitalization-level binary indicator
    print("\nCreating VATS/decortication indicator at hospitalization level...")

    procedures_received = proc_df.groupby('hospitalization_id').size().reset_index()
    procedures_received.columns = ['hospitalization_id', 'procedure_count']
    procedures_received['received_vats_decortication'] = 1

    print(f"OK Hospitalizations with VATS/decortication: {len(procedures_received):,}")
    return (procedures_received,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Apply 5-Day Antibiotic Requirement""")
    return


@app.cell
def _(
    abx_pattern,
    cohort_with_cultures,
    lytics_received,
    pd,
    procedures_received,
):
    # Merge antibiotic patterns, interventions with cohort
    print("\nMerging antibiotic patterns, lytics, and procedures with cohort...")

    # First merge antibiotic patterns
    cohort_with_abx = pd.merge(
        cohort_with_cultures,
        abx_pattern,
        on=['hospitalization_id', 'order_dttm'],
        how='left'
    )

    # Then merge intrapleural lytic statistics (hospitalization-level)
    cohort_with_abx = pd.merge(
        cohort_with_abx,
        lytics_received[['hospitalization_id', 'received_intrapleural_lytic', 'n_doses_alteplase', 'n_doses_dornase_alfa', 'median_dose_alteplase', 'median_dose_dornase_alfa']],
        on='hospitalization_id',
        how='left'
    )

    # Merge VATS/decortication indicator (hospitalization-level, no time window)
    cohort_with_abx = pd.merge(
        cohort_with_abx,
        procedures_received[['hospitalization_id', 'received_vats_decortication']],
        on='hospitalization_id',
        how='left'
    )

    # Fill NaN (no antibiotics) with 0 for binary columns, 5 for free days
    abx_cols = ['day_1_abx', 'day_2_abx', 'day_3_abx', 'day_4_abx', 'day_5_abx', 'all_5_days_abx']
    cohort_with_abx[abx_cols] = cohort_with_abx[abx_cols].fillna(0).astype(int)
    cohort_with_abx['abx_free_days'] = cohort_with_abx['abx_free_days'].fillna(5).astype(int)

    # Fill NaN (no lytics) with 0 for all lytic columns
    lytic_cols = ['received_intrapleural_lytic', 'n_doses_alteplase', 'n_doses_dornase_alfa']
    cohort_with_abx[lytic_cols] = cohort_with_abx[lytic_cols].fillna(0).astype(int)

    # Fill NaN (no lytics) with 0.0 for median dose columns
    median_dose_cols = ['median_dose_alteplase', 'median_dose_dornase_alfa']
    cohort_with_abx[median_dose_cols] = cohort_with_abx[median_dose_cols].fillna(0.0)

    # Fill NaN (no procedures) with 0
    cohort_with_abx['received_vats_decortication'] = cohort_with_abx['received_vats_decortication'].fillna(0).astype(int)

    print(f"OK Cohort with antibiotic patterns, lytics, and procedures: {len(cohort_with_abx):,}")
    print(f"  Before 5-day filter: {len(cohort_with_abx):,} culture orders")
    print(f"  Missing 1+ antibiotic days: {(cohort_with_abx['all_5_days_abx'] == 0).sum():,}")
    print(f"  All 5 antibiotic days covered: {(cohort_with_abx['all_5_days_abx'] == 1).sum():,}")
    print(f"  Received intrapleural lytics: {(cohort_with_abx['received_intrapleural_lytic'] == 1).sum():,} ({(cohort_with_abx['received_intrapleural_lytic'] == 1).sum()/len(cohort_with_abx)*100:.1f}%)")
    print(f"  Received VATS/decortication: {(cohort_with_abx['received_vats_decortication'] == 1).sum():,} ({(cohort_with_abx['received_vats_decortication'] == 1).sum()/len(cohort_with_abx)*100:.1f}%)")

    # Apply 5-day requirement (all 5 days must have antibiotics)
    cohort_final = cohort_with_abx[
        cohort_with_abx['all_5_days_abx'] == 1
    ].copy()

    print(f"\nOK Final cohort after 5-day antibiotic requirement: {len(cohort_final):,}")
    print(f"  Unique hospitalizations: {cohort_final['hospitalization_id'].nunique():,}")
    print(f"  Unique patients: {cohort_final['patient_id'].nunique():,}")
    print(f"  With intrapleural lytics: {(cohort_final['received_intrapleural_lytic'] == 1).sum():,} ({(cohort_final['received_intrapleural_lytic'] == 1).sum()/len(cohort_final)*100:.1f}%)")
    print(f"  With VATS/decortication: {(cohort_final['received_vats_decortication'] == 1).sum():,} ({(cohort_final['received_vats_decortication'] == 1).sum()/len(cohort_final)*100:.1f}%)")
    return cohort_final, cohort_with_abx


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Collapse to First Culture Order per Hospitalization""")
    return


@app.cell
def _(cohort_final):
    # Group by hospitalization, keep first order_dttm, aggregate organisms
    print("\n=== Collapsing to First Culture Order per Hospitalization ===")
    print(f"Before collapse: {len(cohort_final):,} rows (culture orders)")
    print(f"  Unique hospitalizations: {cohort_final['hospitalization_id'].nunique():,}")

    # Sort by hospitalization_id and order_dttm to ensure first order is selected
    cohort_sorted = cohort_final.sort_values(['hospitalization_id', 'order_dttm']).copy()

    # Aggregate organisms: collect all unique organisms across all culture orders per hospitalization
    cohort_first_order = cohort_sorted.groupby('hospitalization_id', as_index=False).agg({
        'patient_id': 'first',
        'age_at_admission': 'first',
        'admission_dttm': 'first',
        'discharge_dttm': 'first',
        'discharge_category': 'first',
        'order_dttm': 'first',  # Keep earliest order_dttm
        'fluid_category': 'first',
        'organism_category': lambda x: '; '.join(sorted(set('; '.join(x).split('; ')))),  # Merge all organisms (distinct only)
        # Antibiotic pattern columns (keep first, should be same if all 5 days covered)
        'day_1_abx': 'first',
        'day_2_abx': 'first',
        'day_3_abx': 'first',
        'day_4_abx': 'first',
        'day_5_abx': 'first',
        'all_5_days_abx': 'first',
        'abx_free_days': 'first',
        # Intervention flags (hospitalization-level, use max for binary 0/1 flags)
        'received_intrapleural_lytic': 'max',
        'n_doses_alteplase': 'first',
        'n_doses_dornase_alfa': 'first',
        'median_dose_alteplase': 'first',
        'median_dose_dornase_alfa': 'first',
        'received_vats_decortication': 'max'
    })

    # Recalculate organism_count from the merged organism_category string
    cohort_first_order['organism_count'] = cohort_first_order['organism_category'].str.count(';') + 1

    # Add fungal culture flag
    cohort_first_order['culture_fungus'] = cohort_first_order['organism_category'].str.lower().str.contains(
        'candida|aspergillus|fungus', na=False
    ).astype(int)

    print(f"\nAfter collapse: {len(cohort_first_order):,} rows (one per hospitalization)")
    print(f"  Unique hospitalizations: {cohort_first_order['hospitalization_id'].nunique():,}")
    print(f"  Unique patients: {cohort_first_order['patient_id'].nunique():,}")

    # Show examples of organism aggregation
    multi_orders = cohort_final.groupby('hospitalization_id').size()
    multi_orders = multi_orders[multi_orders > 1]
    return (cohort_first_order,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Cohort Summary""")
    return


@app.cell
def _(cohort_first_order):
    # Display cohort summary
    print("\n=== Empyema Cohort Summary ===")
    print(f"Total records: {len(cohort_first_order):,}")
    print(f"Unique hospitalizations: {cohort_first_order['hospitalization_id'].nunique():,}")
    print(f"Unique patients: {cohort_first_order['patient_id'].nunique():,}")

    print(f"\n=== Age Distribution ===")
    print(f"Mean age: {cohort_first_order['age_at_admission'].mean():.1f} years")
    print(f"Median age: {cohort_first_order['age_at_admission'].median():.1f} years")
    print(f"Min age: {cohort_first_order['age_at_admission'].min():.0f} years")
    print(f"Max age: {cohort_first_order['age_at_admission'].max():.0f} years")

    print(f"\n=== Date Range ===")
    print(f"First admission: {cohort_first_order['admission_dttm'].min()}")
    print(f"Last admission: {cohort_first_order['admission_dttm'].max()}")

    print(f"\n=== Year Distribution ===")
    _year_dist = cohort_first_order['admission_dttm'].dt.year.value_counts().sort_index()
    for _year, _count in _year_dist.items():
        print(f"  {_year}: {_count:,} hospitalizations")

    print(f"\n=== 5-Day Antibiotic Pattern ===")
    print(f"All 5 days covered: {(cohort_first_order['all_5_days_abx'] == 1).sum():,} (100%)")
    print(f"\nDaily antibiotic coverage:")
    for _day in range(1, 6):
        _covered = (cohort_first_order[f'day_{_day}_abx'] == 1).sum()
        _pct = _covered / len(cohort_first_order) * 100
        print(f"  Day {_day}: {_covered:,} ({_pct:.1f}%)")

    print(f"\n=== Intrapleural Lytic Therapy ===")
    _lytic_count = (cohort_first_order['received_intrapleural_lytic'] == 1).sum()
    _no_lytic_count = (cohort_first_order['received_intrapleural_lytic'] == 0).sum()
    print(f"Received lytics: {_lytic_count:,} ({_lytic_count/len(cohort_first_order)*100:.1f}%)")
    print(f"No lytics: {_no_lytic_count:,} ({_no_lytic_count/len(cohort_first_order)*100:.1f}%)")

    # Show dose statistics for those who received lytics
    if _lytic_count > 0:
        _lytics_df = cohort_first_order[cohort_first_order['received_intrapleural_lytic'] == 1]
        print(f"\nDose statistics for those who received lytics:")
        print(f"  Alteplase doses - Mean: {_lytics_df['n_doses_alteplase'].mean():.1f}, Median: {_lytics_df['n_doses_alteplase'].median():.0f}")
        print(f"  Dornase alfa doses - Mean: {_lytics_df['n_doses_dornase_alfa'].mean():.1f}, Median: {_lytics_df['n_doses_dornase_alfa'].median():.0f}")
        print(f"  Median alteplase dose - Mean: {_lytics_df[_lytics_df['median_dose_alteplase'] > 0]['median_dose_alteplase'].mean():.1f} (n={(_lytics_df['median_dose_alteplase'] > 0).sum()})")
        print(f"  Median dornase alfa dose - Mean: {_lytics_df[_lytics_df['median_dose_dornase_alfa'] > 0]['median_dose_dornase_alfa'].mean():.1f} (n={(_lytics_df['median_dose_dornase_alfa'] > 0).sum()})")

    print(f"\n=== VATS/Decortication Procedures ===")
    _procedure_count = (cohort_first_order['received_vats_decortication'] == 1).sum()
    _no_procedure_count = (cohort_first_order['received_vats_decortication'] == 0).sum()
    print(f"Received VATS/decortication: {_procedure_count:,} ({_procedure_count/len(cohort_first_order)*100:.1f}%)")
    print(f"No VATS/decortication: {_no_procedure_count:,} ({_no_procedure_count/len(cohort_first_order)*100:.1f}%)")

    print(f"\n=== Fungal Cultures ===")
    _fungal_count = (cohort_first_order['culture_fungus'] == 1).sum()
    _no_fungal_count = (cohort_first_order['culture_fungus'] == 0).sum()
    print(f"Fungal organisms: {_fungal_count:,} ({_fungal_count/len(cohort_first_order)*100:.1f}%)")
    print(f"Non-fungal organisms: {_no_fungal_count:,} ({_no_fungal_count/len(cohort_first_order)*100:.1f}%)")

    print(f"\n=== Treatment Modalities ===")
    _only_abx = ((cohort_first_order['received_intrapleural_lytic'] == 0) & (cohort_first_order['received_vats_decortication'] == 0)).sum()
    _abx_lytics = ((cohort_first_order['received_intrapleural_lytic'] == 1) & (cohort_first_order['received_vats_decortication'] == 0)).sum()
    _abx_surgery = ((cohort_first_order['received_intrapleural_lytic'] == 0) & (cohort_first_order['received_vats_decortication'] == 1)).sum()
    _abx_lytics_surgery = ((cohort_first_order['received_intrapleural_lytic'] == 1) & (cohort_first_order['received_vats_decortication'] == 1)).sum()
    print(f"Antibiotics only: {_only_abx:,} ({_only_abx/len(cohort_first_order)*100:.1f}%)")
    print(f"Antibiotics + Lytics: {_abx_lytics:,} ({_abx_lytics/len(cohort_first_order)*100:.1f}%)")
    print(f"Antibiotics + Surgery: {_abx_surgery:,} ({_abx_surgery/len(cohort_first_order)*100:.1f}%)")
    print(f"Antibiotics + Lytics + Surgery: {_abx_lytics_surgery:,} ({_abx_lytics_surgery/len(cohort_first_order)*100:.1f}%)")

    print(f"\n=== Top 10 Organisms ===")
    _org_counts = cohort_first_order['organism_category'].value_counts()
    for _i, (_org, _count) in enumerate(_org_counts.head(10).items(), 1):
        _pct = _count / len(cohort_first_order) * 100
        print(f"  {_i}. {_org}: {_count:,} ({_pct:.1f}%)")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Cohort Filtering Statistics""")
    return


@app.cell
def _(
    cohort_final,
    cohort_first_order,
    cohort_with_abx,
    cohort_with_cultures,
    hosp_df,
    hosp_filtered,
):
    # Create filtering statistics dictionary
    print("\n=== Cohort Filtering Statistics ===")

    filtering_stats = {
        "inclusion_criteria": {
            "age_minimum": 18,
            "admission_date_min": "2018-01-01",
            "admission_date_max": "2024-12-31",
            "discharge_date_max": "2024-12-31",
            "organism_category": "positive (not 'no growth')",
            "fluid_category": "pleural",
            "antibiotics_minimum_days": 5,
            "antibiotics_group": "CMS_sepsis_qualifying_antibiotics"
        },
        "exclusion_criteria": {
            "tuberculosis_mycobacterium": "Entire hospitalization excluded if any culture contains tuberculosis or mycobacterium"
        },
        "filtering_steps": [
            {
                "step": 1,
                "description": "All hospitalizations",
                "total_rows": len(hosp_df),
                "unique_hospitalizations": hosp_df['hospitalization_id'].nunique(),
                "rows_dropped": 0
            },
            {
                "step": 2,
                "description": "Age >=18 & Admission 2018-2024",
                "total_rows": len(hosp_filtered),
                "unique_hospitalizations": hosp_filtered['hospitalization_id'].nunique(),
                "rows_dropped": len(hosp_df) - len(hosp_filtered)
            },
            {
                "step": 3,
                "description": "With positive pleural culture",
                "total_rows": len(cohort_with_cultures),
                "unique_hospitalizations": cohort_with_cultures['hospitalization_id'].nunique(),
                "unique_patients": cohort_with_cultures['patient_id'].nunique(),
                "rows_dropped": len(hosp_filtered) - len(cohort_with_cultures)
            },
            {
                "step": 4,
                "description": "All 5 days IV antibiotics (before collapse)",
                "total_rows": len(cohort_final),
                "unique_hospitalizations": cohort_final['hospitalization_id'].nunique(),
                "unique_patients": cohort_final['patient_id'].nunique(),
                "rows_dropped": len(cohort_with_abx) - len(cohort_final)
            },
            {
                "step": 5,
                "description": "Collapsed to first order per hospitalization",
                "total_rows": len(cohort_first_order),
                "unique_hospitalizations": cohort_first_order['hospitalization_id'].nunique(),
                "unique_patients": cohort_first_order['patient_id'].nunique(),
                "rows_dropped": len(cohort_final) - len(cohort_first_order)
            }
        ],
        "final_cohort": {
            "total_rows": len(cohort_first_order),
            "unique_hospitalizations": cohort_first_order['hospitalization_id'].nunique(),
            "unique_patients": cohort_first_order['patient_id'].nunique(),
            "with_intrapleural_lytics": int((cohort_first_order['received_intrapleural_lytic'] == 1).sum()),
            "with_vats_decortication": int((cohort_first_order['received_vats_decortication'] == 1).sum()),
            "with_fungal_culture": int((cohort_first_order['culture_fungus'] == 1).sum())
        }
    }

    # Print summary
    for step in filtering_stats['filtering_steps']:
        print(f"Step {step['step']}: {step['description']}")
        print(f"  Total rows: {step['total_rows']:,}")
        if 'unique_hospitalizations' in step:
            print(f"  Unique hospitalizations: {step['unique_hospitalizations']:,}")
        if 'unique_patients' in step:
            print(f"  Unique patients: {step['unique_patients']:,}")
        if step['rows_dropped'] > 0:
            print(f"  Rows dropped: {step['rows_dropped']:,}")
        print()

    print(f"Final cohort:")
    print(f"  Total rows: {filtering_stats['final_cohort']['total_rows']:,}")
    print(f"  Unique hospitalizations: {filtering_stats['final_cohort']['unique_hospitalizations']:,}")
    print(f"  Unique patients: {filtering_stats['final_cohort']['unique_patients']:,}")
    print(f"  With intrapleural lytics: {filtering_stats['final_cohort']['with_intrapleural_lytics']:,}")
    print(f"  With VATS/decortication: {filtering_stats['final_cohort']['with_vats_decortication']:,}")
    print(f"  With fungal culture: {filtering_stats['final_cohort']['with_fungal_culture']:,}")
    return (filtering_stats,)


@app.cell
def _(filtering_stats):
    import json as _json
    from pathlib import Path as _json_Path

    # Save filtering statistics to JSON
    _json_output_path = _json_Path('PHI_DATA') / 'cohort_filtering_stats.json'
    _json_output_path.parent.mkdir(exist_ok=True)

    with open(_json_output_path, 'w') as _f:
        _json.dump(filtering_stats, _f, indent=2)

    print(f"\n=== Filtering Statistics Saved ===")
    print(f"Location: {_json_output_path}")
    print(f"Format: JSON with 2-space indentation")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Save Cohort""")
    return


@app.cell
def _(cohort_first_order):
    from pathlib import Path

    # Create PHI_DATA directory
    phi_data_dir = Path('PHI_DATA')
    phi_data_dir.mkdir(exist_ok=True)

    # Save cohort to parquet
    output_path = phi_data_dir / 'cohort_empyema_initial.parquet'
    cohort_first_order.to_parquet(output_path, index=False)

    print(f"\n=== Cohort Saved ===")
    print(f"Location: {output_path}")
    print(f"Rows: {len(cohort_first_order):,}")
    print(f"Columns: {len(cohort_first_order.columns)}")
    print(f"File size: {output_path.stat().st_size / (1024**2):.2f} MB")
    print(f"\nColumns: {list(cohort_first_order.columns)}")
    return


if __name__ == "__main__":
    app.run()
