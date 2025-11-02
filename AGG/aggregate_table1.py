#!/usr/bin/env python3
"""
Aggregate table1_statistics_by_treatment.json files from multiple sites
into a single CSV file with pooled statistics.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
import numpy as np


class StatParser:
    """Parse different statistical formats from JSON strings."""

    @staticmethod
    def parse_mean_sd(value: str) -> Optional[Tuple[float, float]]:
        """Parse 'mean ± SD' format."""
        if not isinstance(value, str) or value in ['nan', '<NA>', '']:
            return None
        match = re.match(r'([-\d.]+)\s*±\s*([-\d.]+)', value)
        if match:
            return float(match.group(1)), float(match.group(2))
        return None

    @staticmethod
    def parse_median_iqr(value: str) -> Optional[Tuple[float, float, float]]:
        """Parse 'median [Q1, Q3]' format."""
        if not isinstance(value, str) or value in ['nan', '<NA>', '']:
            return None
        match = re.match(r'([-\d.]+)\s*\[([-\d.]+),\s*([-\d.]+)\]', value)
        if match:
            return float(match.group(1)), float(match.group(2)), float(match.group(3))
        return None

    @staticmethod
    def parse_count_pct(value: str) -> Optional[Tuple[int, float]]:
        """Parse 'count (percentage%)' format."""
        if not isinstance(value, str) or value in ['nan', '<NA>', '']:
            return None
        match = re.match(r'(\d+)\s*\(([\d.]+)%?\)', value)
        if match:
            return int(match.group(1)), float(match.group(2))
        return None

    @staticmethod
    def parse_n(value: str) -> Optional[int]:
        """Parse plain number string."""
        if not isinstance(value, str) or value in ['nan', '<NA>', '']:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    @staticmethod
    def format_mean_sd(mean: float, sd: float) -> str:
        """Format mean and SD back to string."""
        return f"{mean:.1f} ± {sd:.1f}"

    @staticmethod
    def format_median_iqr(median: float, q1: float, q3: float) -> str:
        """Format median and IQR back to string."""
        return f"{median:.1f} [{q1:.1f}, {q3:.1f}]"

    @staticmethod
    def format_count_pct(count: int, pct: float) -> str:
        """Format count and percentage back to string."""
        return f"{count} ({pct:.1f}%)"


class TableAggregator:
    """Aggregate statistics across multiple sites."""

    def __init__(self):
        self.parser = StatParser()

    def aggregate_n(self, values: List[Optional[int]]) -> str:
        """Sum N values across sites."""
        valid = [v for v in values if v is not None]
        if not valid:
            return "0"
        return str(sum(valid))

    def aggregate_means(self, values: List[Optional[Tuple[float, float]]]) -> str:
        """Simple average of means across sites."""
        valid = [v for v in values if v is not None]
        if not valid:
            return "nan"

        means = [v[0] for v in valid]
        sds = [v[1] for v in valid]

        avg_mean = np.mean(means)
        avg_sd = np.mean(sds)

        return self.parser.format_mean_sd(avg_mean, avg_sd)

    def aggregate_medians(self, values: List[Optional[Tuple[float, float, float]]]) -> str:
        """Median of medians with combined IQR."""
        valid = [v for v in values if v is not None]
        if not valid:
            return "nan"

        medians = [v[0] for v in valid]
        q1s = [v[1] for v in valid]
        q3s = [v[2] for v in valid]

        pooled_median = np.median(medians)
        pooled_q1 = np.median(q1s)
        pooled_q3 = np.median(q3s)

        return self.parser.format_median_iqr(pooled_median, pooled_q1, pooled_q3)

    def aggregate_counts(self, values: List[Optional[Tuple[int, float]]],
                        total_n: int) -> str:
        """Sum counts and recalculate percentage."""
        valid = [v for v in values if v is not None]
        if not valid or total_n == 0:
            return "0 (0.0%)"

        total_count = sum(v[0] for v in valid)
        pct = (total_count / total_n) * 100

        return self.parser.format_count_pct(total_count, pct)


def find_table1_files(base_dir: Path) -> List[Path]:
    """Find all table1_statistics_by_treatment.json files."""
    files = list(base_dir.glob('**/table1_statistics_by_treatment.json'))
    return sorted(files)


def normalize_field_names(cohort_data: Dict) -> Dict:
    """Normalize field names to handle variations across sites."""
    normalized = {}

    for key, value in cohort_data.items():
        # Normalize Sex field capitalization
        if key == 'Sex: male':
            key = 'Sex: Male'
        elif key == 'Sex: female':
            key = 'Sex: Female'

        normalized[key] = value

    return normalized


def load_json_files(files: List[Path]) -> List[Dict]:
    """Load all JSON files and normalize field names."""
    data = []
    for file in files:
        with open(file, 'r') as f:
            site_data = json.load(f)

            # Normalize field names in each cohort
            for cohort_name in site_data['cohort_groups']:
                site_data['cohort_groups'][cohort_name] = normalize_field_names(
                    site_data['cohort_groups'][cohort_name]
                )

            data.append(site_data)
    return data


def get_all_field_names(data: List[Dict]) -> List[str]:
    """Extract all unique field names across all sites and cohorts."""
    fields = set()
    for site_data in data:
        for cohort_name, cohort_data in site_data['cohort_groups'].items():
            fields.update(cohort_data.keys())

    # Sort fields in a logical order
    priority_fields = [
        'N', 'Unique Patients', 'N with ICU stay',
        'Age (mean ± SD)', 'Age (median [IQR])',
        'BMI (mean ± SD)', 'BMI (median [IQR])',
    ]

    sorted_fields = []
    for field in priority_fields:
        if field in fields:
            sorted_fields.append(field)
            fields.remove(field)

    # Add remaining fields alphabetically
    sorted_fields.extend(sorted(fields))

    return sorted_fields


def aggregate_data(data: List[Dict]) -> pd.DataFrame:
    """Aggregate data from all sites into a single DataFrame."""
    aggregator = TableAggregator()
    parser = StatParser()

    cohorts = ['antibiotics_only', 'intrapleural_lytics', 'vats_cohort', 'total']
    all_fields = get_all_field_names(data)

    result = {}

    for cohort in cohorts:
        result[cohort] = {}

        # First pass: get total N for this cohort
        n_values = []
        for site_data in data:
            cohort_data = site_data['cohort_groups'].get(cohort, {})
            n_val = parser.parse_n(cohort_data.get('N', '0'))
            n_values.append(n_val)

        total_n_str = aggregator.aggregate_n(n_values)
        total_n = int(total_n_str) if total_n_str != '0' else 0

        # Second pass: aggregate all fields
        for field in all_fields:
            values = []

            # Collect values from all sites
            for site_data in data:
                cohort_data = site_data['cohort_groups'].get(cohort, {})
                value = cohort_data.get(field)
                values.append(value)

            # Determine field type and aggregate
            if (field == 'N' or field.startswith('N ') or
                field == 'Unique Patients' or 'Patients' in field):
                parsed = [parser.parse_n(v) for v in values]
                result[cohort][field] = aggregator.aggregate_n(parsed)

            elif '(mean ± SD)' in field:
                parsed = [parser.parse_mean_sd(v) for v in values]
                result[cohort][field] = aggregator.aggregate_means(parsed)

            elif '(median [IQR])' in field:
                parsed = [parser.parse_median_iqr(v) for v in values]
                result[cohort][field] = aggregator.aggregate_medians(parsed)

            else:
                # Assume it's a count/percentage format
                parsed = [parser.parse_count_pct(v) for v in values]
                result[cohort][field] = aggregator.aggregate_counts(parsed, total_n)

    # Convert to DataFrame
    df = pd.DataFrame(result)
    df.index.name = 'Variable'

    return df


def create_site_based_tables(data: List[Dict], cohort_name: str) -> pd.DataFrame:
    """Create a table with sites as columns for a specific cohort."""
    all_fields = get_all_field_names(data)

    # Extract site names from data
    site_names = [site_data['site_name'] for site_data in data]

    result = {}

    for site_data, site_name in zip(data, site_names):
        cohort_data = site_data['cohort_groups'].get(cohort_name, {})
        result[site_name] = {}

        for field in all_fields:
            # Get the value directly without aggregation
            value = cohort_data.get(field, '')
            # Handle None and convert to string
            if value is None or value == 'nan':
                value = 'nan'
            result[site_name][field] = value

    # Convert to DataFrame
    df = pd.DataFrame(result)
    df.index.name = 'Variable'

    return df


def main():
    """Main function to aggregate table1 data."""
    # Find base directory
    script_dir = Path(__file__).parent
    base_dir = script_dir

    print(f"Searching for table1 files in: {base_dir}")

    # Find all table1 JSON files
    files = find_table1_files(base_dir)
    print(f"\nFound {len(files)} files:")
    for f in files:
        print(f"  - {f.relative_to(base_dir)}")

    if not files:
        print("No table1_statistics_by_treatment.json files found!")
        return

    # Load JSON data
    print("\nLoading JSON files...")
    data = load_json_files(files)

    # 1. Create cohort-based aggregation (aggregates across sites)
    print("\n[1/4] Aggregating statistics across sites...")
    df_cohorts = aggregate_data(data)

    output_file = base_dir / 'aggregated_table1.csv'
    df_cohorts.to_csv(output_file)

    print(f"✓ Cohort-based table saved to: {output_file}")
    print(f"  Shape: {df_cohorts.shape[0]} variables × {df_cohorts.shape[1]} cohorts")

    # 2. Create site-based tables for each cohort
    cohorts_to_export = [
        ('antibiotics_only', 'table1_antibiotics_only.csv'),
        ('intrapleural_lytics', 'table1_intrapleural_lytics.csv'),
        ('vats_cohort', 'table1_vats_cohort.csv')
    ]

    for idx, (cohort_name, filename) in enumerate(cohorts_to_export, start=2):
        print(f"\n[{idx}/4] Creating site-based table for {cohort_name}...")
        df_sites = create_site_based_tables(data, cohort_name)

        output_file = base_dir / filename
        df_sites.to_csv(output_file)

        print(f"✓ Site-based table saved to: {output_file}")
        print(f"  Shape: {df_sites.shape[0]} variables × {df_sites.shape[1]} sites")

    print("\n" + "="*60)
    print("All tables generated successfully!")
    print("="*60)
    print("\nOutput files:")
    print("  1. aggregated_table1.csv - Aggregated across sites, cohorts as columns")
    print("  2. table1_antibiotics_only.csv - Antibiotics cohort, sites as columns")
    print("  3. table1_intrapleural_lytics.csv - Lytics cohort, sites as columns")
    print("  4. table1_vats_cohort.csv - VATS cohort, sites as columns")


if __name__ == '__main__':
    main()
