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

    ## Objective
    Generate a cohort of adult patients with:
    - Positive pleural fluid culture (organism_category != 'no growth')
    - Age ≥18 years
    - Hospitalization between 2018-2024
    - Received ≥5 days of IV antibiotics after culture order

    ## Inclusion Criteria
    - Adult patients aged ≥18 years
    - Positive organism_category in fluid_category == 'pleural' (not "no growth")
    - Received at least 5 days of IV antibiotics after order_dttm of positive culture
    - Admission date between 2018-2024
    - Discharge date <= 2024

    ## Exclusion Criteria (to be implemented in next notebook)
    - Hospitalization in prior 6 weeks with positive pleural culture
    - Fewer than 5 days of IV antibiotics after order_dttm
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

    print(f"✓ Hospitalization data loaded: {len(hosp_df):,} records")

    # Convert datetime columns
    hosp_df['admission_dttm'] = pd.to_datetime(hosp_df['admission_dttm'])
    hosp_df['discharge_dttm'] = pd.to_datetime(hosp_df['discharge_dttm'])

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

    print(f"  After age filter (≥18): {len(hosp_filtered):,}")
    print(f"  After date filters (2018-2024 admission, ≤2024 discharge): {len(hosp_filtered):,}")
    print(f"✓ Filtered hospitalizations: {len(hosp_filtered):,}")
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
    print(f"✓ Microbiology culture data loaded: {len(micro_df):,} records")

    # Convert hospitalization_id format (Rush-specific fix for .0 suffix)
    if site_name == 'rush':
        print("\nConverting hospitalization_id format (Rush-specific)...")
        micro_df['hospitalization_id'] = micro_df['hospitalization_id'].astype(float).astype(int).astype(str)
        print(f"✓ Hospitalization IDs converted to clean string format (removed .0 suffix)")


    # Filter to only eligible hospitalization IDs
    print(f"\nFiltering to eligible hospitalizations...")
    print(f"  Before filter: {len(micro_df):,} records")
    micro_df = micro_df[micro_df['hospitalization_id'].isin(eligible_hosp_ids)].copy()
    print(f"  After filter: {len(micro_df):,} records")

    # Filter to pleural fluid only
    print("\nApplying pleural fluid filter...")
    print(f"  Before pleural filter: {len(micro_df):,}")

    # Filter to pleural (looking for 'pleural' in fluid_category)
    micro_pleural = micro_df[
        micro_df['fluid_category'].str.lower().str.contains('pleural', na=False)
    ].copy()

    print(f"  After pleural filter: {len(micro_pleural):,}")

    # Exclude 'no growth'
    print(f"\nExcluding 'no growth' organisms...")
    print(f"  Before no_growth filter: {len(micro_pleural):,}")

    micro_positive = micro_pleural[
        micro_pleural['organism_category'].str.lower() != 'no_growth'
    ].copy()

    print(f"  After no_growth filter: {len(micro_positive):,}")
    print(f"✓ Positive pleural cultures: {len(micro_positive):,}")
    return (micro_positive,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Group Organisms by Culture Event""")
    return


@app.cell
def _(micro_positive):
    # Group by culture event to prevent duplicate rows from polymicrobial cultures
    # (same patient, hospitalization, order time, fluid → multiple organisms)
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

    print(f"✓ Grouped to one row per culture event")
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

    print(f"✓ Hospitalizations with positive pleural cultures: {len(cohort_with_cultures):,}")
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

    meds_df = meds_table.df[meds_table.df['med_group'] == 'CMS_sepsis_qualifying_antibiotics'].copy()
    print(f"✓ CMS sepsis-qualifying antibiotics loaded: {len(meds_df):,} records")
    return cohort_hosp_ids, meds_df


@app.cell
def _(cohort_with_cultures, meds_df, pd):
    # Filter antibiotics to those given AFTER pleural culture order_dttm
    print("\nFiltering antibiotics to post-culture administration...")

    # Convert datetime
    meds_df['admin_dttm'] = pd.to_datetime(meds_df['admin_dttm'])

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

    print(f"✓ Antibiotic administrations after culture order: {len(abx_post_culture):,}")
    print(f"  Unique hospitalizations: {abx_post_culture['hospitalization_id'].nunique():,}")
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

    print(f"✓ Antibiotics in 5-day window: {len(abx_5day_window):,}")
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

    # Use tqdm for progress tracking
    tqdm.pandas(desc="Processing culture orders")
    abx_pattern = abx_5day_window.groupby(['hospitalization_id', 'order_dttm']).progress_apply(calculate_5day_pattern).reset_index()

    print(f"\n✓ Pattern calculated for {len(abx_pattern):,} culture orders")
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
        columns=['hospitalization_id', 'admin_dttm', 'med_category', 'med_route_category']
    )

    # Filter to intrapleural lytics (alteplase or dornase alfa)
    intrapleural_df = intrapleural_table.df[
        (intrapleural_table.df['med_route_category'] == 'intrapleural') &
        (intrapleural_table.df['med_category'].isin(['alteplase', 'dornase_alfa']))
    ].copy()

    print(f"✓ Intrapleural lytics loaded: {len(intrapleural_df):,} records")
    print(f"  Alteplase: {(intrapleural_df['med_category'] == 'alteplase').sum():,}")
    print(f"  Dornase alfa: {(intrapleural_df['med_category'] == 'dornase_alfa').sum():,}")
    return intrapleural_df, intrapleural_table


@app.cell
def _(intrapleural_table):
    intrapleural_table.df.med_category.unique()
    return


@app.cell
def _(cohort_with_cultures, intrapleural_df, pd):
    # Merge with cohort to get order_dttm for each hospitalization
    print("\nFiltering intrapleural lytics to 5-day window from culture order...")

    intrapleural_with_order = pd.merge(
        intrapleural_df,
        cohort_with_cultures[['hospitalization_id', 'order_dttm']],
        on='hospitalization_id',
        how='inner'
    )

    # Convert datetime
    intrapleural_with_order['admin_dttm'] = pd.to_datetime(intrapleural_with_order['admin_dttm'])

    # Filter to 5-day window from order_dttm
    intrapleural_5day = intrapleural_with_order[
        (intrapleural_with_order['admin_dttm'] >= intrapleural_with_order['order_dttm']) &
        ((intrapleural_with_order['admin_dttm'] - intrapleural_with_order['order_dttm']).dt.total_seconds() <= (5 * 24 * 3600))
    ].copy()

    print(f"✓ Intrapleural lytics in 5-day window: {len(intrapleural_5day):,}")
    print(f"  Unique culture orders with lytics: {intrapleural_5day.groupby(['hospitalization_id', 'order_dttm']).ngroups:,}")
    return (intrapleural_5day,)


@app.cell
def _(intrapleural_5day):
    # Create binary indicator: did they receive ANY intrapleural lytic in 5-day window?
    print("\nCreating intrapleural lytic indicator...")

    lytics_received = intrapleural_5day.groupby(['hospitalization_id', 'order_dttm']).size().reset_index()
    lytics_received.columns = ['hospitalization_id', 'order_dttm', 'lytic_admin_count']
    lytics_received['received_intrapleural_lytic'] = 1

    print(f"✓ Culture orders with intrapleural lytics: {len(lytics_received):,}")
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
        columns=['hospitalization_id', 'procedure_code', 'procedure_code_format', 'procedure_billed_dttm']
    )

    # Filter to VATS/decortication CPT codes
    vats_cpt_codes = ['32651', '32652', '32225', '32220', '32320']
    proc_df = proc_table.df[
        (proc_table.df['procedure_code_format'] == 'CPT') &
        (proc_table.df['procedure_code'].isin(vats_cpt_codes))
    ].copy()

    print(f"✓ VATS/Decortication procedures loaded: {len(proc_df):,} records")
    if len(proc_df) > 0:
        print(f"  CPT code distribution:")
        for _code in vats_cpt_codes:
            _code_count = (proc_df['procedure_code'] == _code).sum()
            if _code_count > 0:
                print(f"    {_code}: {_code_count:,}")
    return (proc_df,)


@app.cell
def _(proc_df):
    # Create hospitalization-level binary indicator
    print("\nCreating VATS/decortication indicator at hospitalization level...")

    procedures_received = proc_df.groupby('hospitalization_id').size().reset_index()
    procedures_received.columns = ['hospitalization_id', 'procedure_count']
    procedures_received['received_vats_decortication'] = 1

    print(f"✓ Hospitalizations with VATS/decortication: {len(procedures_received):,}")
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

    # Then merge intrapleural lytic indicator
    cohort_with_abx = pd.merge(
        cohort_with_abx,
        lytics_received[['hospitalization_id', 'order_dttm', 'received_intrapleural_lytic']],
        on=['hospitalization_id', 'order_dttm'],
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

    # Fill NaN (no lytics) with 0
    cohort_with_abx['received_intrapleural_lytic'] = cohort_with_abx['received_intrapleural_lytic'].fillna(0).astype(int)

    # Fill NaN (no procedures) with 0
    cohort_with_abx['received_vats_decortication'] = cohort_with_abx['received_vats_decortication'].fillna(0).astype(int)

    print(f"✓ Cohort with antibiotic patterns, lytics, and procedures: {len(cohort_with_abx):,}")
    print(f"  Before 5-day filter: {len(cohort_with_abx):,} culture orders")
    print(f"  Missing 1+ antibiotic days: {(cohort_with_abx['all_5_days_abx'] == 0).sum():,}")
    print(f"  All 5 antibiotic days covered: {(cohort_with_abx['all_5_days_abx'] == 1).sum():,}")
    print(f"  Received intrapleural lytics: {(cohort_with_abx['received_intrapleural_lytic'] == 1).sum():,} ({(cohort_with_abx['received_intrapleural_lytic'] == 1).sum()/len(cohort_with_abx)*100:.1f}%)")
    print(f"  Received VATS/decortication: {(cohort_with_abx['received_vats_decortication'] == 1).sum():,} ({(cohort_with_abx['received_vats_decortication'] == 1).sum()/len(cohort_with_abx)*100:.1f}%)")

    # Apply 5-day requirement (all 5 days must have antibiotics)
    cohort_final = cohort_with_abx[
        cohort_with_abx['all_5_days_abx'] == 1
    ].copy()

    print(f"\n✓ Final cohort after 5-day antibiotic requirement: {len(cohort_final):,}")
    print(f"  Unique hospitalizations: {cohort_final['hospitalization_id'].nunique():,}")
    print(f"  Unique patients: {cohort_final['patient_id'].nunique():,}")
    print(f"  With intrapleural lytics: {(cohort_final['received_intrapleural_lytic'] == 1).sum():,} ({(cohort_final['received_intrapleural_lytic'] == 1).sum()/len(cohort_final)*100:.1f}%)")
    print(f"  With VATS/decortication: {(cohort_final['received_vats_decortication'] == 1).sum():,} ({(cohort_final['received_vats_decortication'] == 1).sum()/len(cohort_final)*100:.1f}%)")
    return cohort_final, cohort_with_abx


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Cohort Summary""")
    return


@app.cell
def _(cohort_final):
    # Display cohort summary
    print("\n=== Empyema Cohort Summary ===")
    print(f"Total records: {len(cohort_final):,}")
    print(f"Unique hospitalizations: {cohort_final['hospitalization_id'].nunique():,}")
    print(f"Unique patients: {cohort_final['patient_id'].nunique():,}")

    print(f"\n=== Age Distribution ===")
    print(f"Mean age: {cohort_final['age_at_admission'].mean():.1f} years")
    print(f"Median age: {cohort_final['age_at_admission'].median():.1f} years")
    print(f"Min age: {cohort_final['age_at_admission'].min():.0f} years")
    print(f"Max age: {cohort_final['age_at_admission'].max():.0f} years")

    print(f"\n=== Date Range ===")
    print(f"First admission: {cohort_final['admission_dttm'].min()}")
    print(f"Last admission: {cohort_final['admission_dttm'].max()}")

    print(f"\n=== Year Distribution ===")
    _year_dist = cohort_final['admission_dttm'].dt.year.value_counts().sort_index()
    for _year, _count in _year_dist.items():
        print(f"  {_year}: {_count:,} hospitalizations")

    print(f"\n=== 5-Day Antibiotic Pattern ===")
    print(f"All 5 days covered: {(cohort_final['all_5_days_abx'] == 1).sum():,} (100%)")
    print(f"\nDaily antibiotic coverage:")
    for _day in range(1, 6):
        _covered = (cohort_final[f'day_{_day}_abx'] == 1).sum()
        _pct = _covered / len(cohort_final) * 100
        print(f"  Day {_day}: {_covered:,} ({_pct:.1f}%)")

    print(f"\n=== Intrapleural Lytic Therapy ===")
    _lytic_count = (cohort_final['received_intrapleural_lytic'] == 1).sum()
    _no_lytic_count = (cohort_final['received_intrapleural_lytic'] == 0).sum()
    print(f"Received lytics: {_lytic_count:,} ({_lytic_count/len(cohort_final)*100:.1f}%)")
    print(f"No lytics: {_no_lytic_count:,} ({_no_lytic_count/len(cohort_final)*100:.1f}%)")

    print(f"\n=== VATS/Decortication Procedures ===")
    _procedure_count = (cohort_final['received_vats_decortication'] == 1).sum()
    _no_procedure_count = (cohort_final['received_vats_decortication'] == 0).sum()
    print(f"Received VATS/decortication: {_procedure_count:,} ({_procedure_count/len(cohort_final)*100:.1f}%)")
    print(f"No VATS/decortication: {_no_procedure_count:,} ({_no_procedure_count/len(cohort_final)*100:.1f}%)")

    print(f"\n=== Treatment Modalities ===")
    _only_abx = ((cohort_final['received_intrapleural_lytic'] == 0) & (cohort_final['received_vats_decortication'] == 0)).sum()
    _abx_lytics = ((cohort_final['received_intrapleural_lytic'] == 1) & (cohort_final['received_vats_decortication'] == 0)).sum()
    _abx_surgery = ((cohort_final['received_intrapleural_lytic'] == 0) & (cohort_final['received_vats_decortication'] == 1)).sum()
    _abx_lytics_surgery = ((cohort_final['received_intrapleural_lytic'] == 1) & (cohort_final['received_vats_decortication'] == 1)).sum()
    print(f"Antibiotics only: {_only_abx:,} ({_only_abx/len(cohort_final)*100:.1f}%)")
    print(f"Antibiotics + Lytics: {_abx_lytics:,} ({_abx_lytics/len(cohort_final)*100:.1f}%)")
    print(f"Antibiotics + Surgery: {_abx_surgery:,} ({_abx_surgery/len(cohort_final)*100:.1f}%)")
    print(f"Antibiotics + Lytics + Surgery: {_abx_lytics_surgery:,} ({_abx_lytics_surgery/len(cohort_final)*100:.1f}%)")

    print(f"\n=== Top 10 Organisms ===")
    _org_counts = cohort_final['organism_category'].value_counts()
    for _i, (_org, _count) in enumerate(_org_counts.head(10).items(), 1):
        _pct = _count / len(cohort_final) * 100
        print(f"  {_i}. {_org}: {_count:,} ({_pct:.1f}%)")
    return


@app.cell
def _(mo):
    mo.md(r"""## CONSORT Diagram""")
    return


@app.cell
def _(
    cohort_final,
    cohort_with_abx,
    cohort_with_cultures,
    hosp_df,
    hosp_filtered,
):
    # Collect all counts for CONSORT diagram
    print("\n=== CONSORT Diagram Counts ===")

    consort_data = {
        'all_hosp': len(hosp_df),
        'after_age_date': len(hosp_filtered),
        'excluded_age_date': len(hosp_df) - len(hosp_filtered),
        'with_positive_pleural': len(cohort_with_cultures),
        'excluded_no_pleural': len(hosp_filtered) - cohort_with_cultures['hospitalization_id'].nunique(),
        'before_5day_filter': len(cohort_with_abx),
        'final_cohort': len(cohort_final),
        'excluded_insufficient_abx': len(cohort_with_abx) - len(cohort_final)
    }

    print(f"All hospitalizations: {consort_data['all_hosp']:,}")
    print(f"  Excluded (age <18 or dates): {consort_data['excluded_age_date']:,}")
    print(f"After age/date filter: {consort_data['after_age_date']:,}")
    print(f"  Excluded (no positive pleural culture): {consort_data['excluded_no_pleural']:,}")
    print(f"With positive pleural culture: {consort_data['with_positive_pleural']:,}")
    print(f"  Excluded (<5 days IV antibiotics): {consort_data['excluded_insufficient_abx']:,}")
    print(f"Final cohort: {consort_data['final_cohort']:,}")
    return (consort_data,)


@app.cell
def _(consort_data):
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

    print("\nCreating CONSORT diagram...")

    fig, ax = plt.subplots(figsize=(10, 12))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 12)
    ax.axis('off')

    # Define positions
    y_start = 11
    y_step = 2.5
    box_width = 6
    box_height = 1.2
    x_center = 5

    # Box 1: All hospitalizations
    y1 = y_start
    box1 = FancyBboxPatch(
        (x_center - box_width/2, y1 - box_height/2), box_width, box_height,
        boxstyle="round,pad=0.1", edgecolor='black', facecolor='lightblue', linewidth=2
    )
    ax.add_patch(box1)
    ax.text(x_center, y1, f"All Hospitalizations\n(n={consort_data['all_hosp']:,})",
            ha='center', va='center', fontsize=11, weight='bold')

    # Exclusion 1
    y_excl1 = y1 - y_step/2
    ax.text(x_center - box_width/2 - 0.5, y_excl1,
            f"Excluded: Age <18 or\nadmission outside 2018-2024\n(n={consort_data['excluded_age_date']:,})",
            ha='right', va='center', fontsize=9, style='italic')

    # Arrow 1
    arrow1 = FancyArrowPatch(
        (x_center, y1 - box_height/2), (x_center, y1 - y_step + box_height/2),
        arrowstyle='->', mutation_scale=20, linewidth=2, color='black'
    )
    ax.add_patch(arrow1)

    # Box 2: After age/date filter
    y2 = y1 - y_step
    box2 = FancyBboxPatch(
        (x_center - box_width/2, y2 - box_height/2), box_width, box_height,
        boxstyle="round,pad=0.1", edgecolor='black', facecolor='lightblue', linewidth=2
    )
    ax.add_patch(box2)
    ax.text(x_center, y2, f"Age ≥18 & Admission 2018-2024\n(n={consort_data['after_age_date']:,})",
            ha='center', va='center', fontsize=11, weight='bold')

    # Exclusion 2
    y_excl2 = y2 - y_step/2
    ax.text(x_center - box_width/2 - 0.5, y_excl2,
            f"Excluded: No positive\npleural culture\n(n={consort_data['excluded_no_pleural']:,})",
            ha='right', va='center', fontsize=9, style='italic')

    # Arrow 2
    arrow2 = FancyArrowPatch(
        (x_center, y2 - box_height/2), (x_center, y2 - y_step + box_height/2),
        arrowstyle='->', mutation_scale=20, linewidth=2, color='black'
    )
    ax.add_patch(arrow2)

    # Box 3: With positive pleural culture
    y3 = y2 - y_step
    box3 = FancyBboxPatch(
        (x_center - box_width/2, y3 - box_height/2), box_width, box_height,
        boxstyle="round,pad=0.1", edgecolor='black', facecolor='lightblue', linewidth=2
    )
    ax.add_patch(box3)
    ax.text(x_center, y3, f"With Positive Pleural Culture\n(n={consort_data['with_positive_pleural']:,})",
            ha='center', va='center', fontsize=11, weight='bold')

    # Exclusion 3
    y_excl3 = y3 - y_step/2
    ax.text(x_center - box_width/2 - 0.5, y_excl3,
            f"Excluded: <5 days\nIV antibiotics\n(n={consort_data['excluded_insufficient_abx']:,})",
            ha='right', va='center', fontsize=9, style='italic')

    # Arrow 3
    arrow3 = FancyArrowPatch(
        (x_center, y3 - box_height/2), (x_center, y3 - y_step + box_height/2),
        arrowstyle='->', mutation_scale=20, linewidth=2, color='black'
    )
    ax.add_patch(arrow3)

    # Box 4: Final cohort
    y4 = y3 - y_step
    box4 = FancyBboxPatch(
        (x_center - box_width/2, y4 - box_height/2), box_width, box_height,
        boxstyle="round,pad=0.1", edgecolor='darkgreen', facecolor='lightgreen', linewidth=3
    )
    ax.add_patch(box4)
    ax.text(x_center, y4, f"FINAL COHORT\n(n={consort_data['final_cohort']:,})",
            ha='center', va='center', fontsize=12, weight='bold')

    # Title
    ax.text(x_center, y_start + 0.8, 'CONSORT Diagram: Empyema Cohort Selection',
            ha='center', va='center', fontsize=14, weight='bold')

    plt.tight_layout()
    print("✓ CONSORT diagram created")
    return (fig,)


@app.cell
def _(fig):
    from pathlib import Path as _Path

    # Save CONSORT diagram
    _output_path = _Path('PHI_DATA') / 'consort_diagram.png'
    fig.savefig(_output_path, dpi=300, bbox_inches='tight', facecolor='white')

    print(f"\n=== CONSORT Diagram Saved ===")
    print(f"Location: {_output_path}")
    print(f"Resolution: 300 dpi")
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Save Cohort""")
    return


@app.cell
def _(cohort_final):
    from pathlib import Path

    # Create PHI_DATA directory
    phi_data_dir = Path('PHI_DATA')
    phi_data_dir.mkdir(exist_ok=True)

    # Save cohort to parquet
    output_path = phi_data_dir / 'cohort_empyema_initial.parquet'
    cohort_final.to_parquet(output_path, index=False)

    print(f"\n=== Cohort Saved ===")
    print(f"Location: {output_path}")
    print(f"Rows: {len(cohort_final):,}")
    print(f"Columns: {len(cohort_final.columns)}")
    print(f"File size: {output_path.stat().st_size / (1024**2):.2f} MB")
    print(f"\nColumns: {list(cohort_final.columns)}")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
