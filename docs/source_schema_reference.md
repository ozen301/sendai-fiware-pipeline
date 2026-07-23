# Source Schema Reference

Raw source-table schema snapshots for the Sendai FIWARE pipeline. Keep
`pipeline_spec.md` focused on load-bearing semantics; use this reference when
full `DESC` output or sample rows are needed.

## Product A -- `flow_metrics2_per_place2_agg_imputed`

This table is a gap-filled superset of `flow_metrics2_per_place2_agg`. Every
original aggregate row is present with `imputed_flag = 0`, and additional
interpolated rows use `imputed_flag = 1`.

```sql
mysql> desc bleData2025d.flow_metrics2_per_place2_agg_imputed;
+--------------------------+-----------------+------+-----+-------------------+
| Field                    | Type            | Null | Key | Default           |
+--------------------------+-----------------+------+-----+-------------------+
| id                       | bigint unsigned | NO   | PRI | auto_increment    |
| startdate                | varchar(20)     | NO   | MUL | NULL              |  -- 'YYYYMMDD_HHMM'
| group_place_id           | varchar(64)     | NO   | MUL | NULL              |  -- e.g. 'sendai2023.10'
| chip_id                  | varchar(64)     | NO   |     | NULL              |
| device_type              | varchar(32)     | NO   |     | NULL              |  -- 'M5Stack' or 'Pixel3aUT'
| interval_min             | int             | NO   |     | NULL              |  -- 1, 5, or 60
| actual_minutes           | decimal(5,2)    | YES  |     | NULL              |
| recorded_at              | datetime        | NO   | MUL | NULL              |
| flow_gt_m40              | int             | YES  |     | NULL              |  -- count: signal stronger than -40 dBm
| flow_gt_m50              | int             | YES  |     | NULL              |
| flow_gt_m60              | int             | YES  |     | NULL              |
| flow_gt_m70              | int             | YES  |     | NULL              |
| flow_gt_m80              | int             | YES  |     | NULL              |
| flow_gt_m90              | int             | YES  |     | NULL              |
| flow_gt_m100             | int             | YES  |     | NULL              |
| flow_gt_m110             | int             | YES  |     | NULL              |
| flow_gt_m120             | int             | YES  |     | NULL              |
| stay_gt_m40              | decimal(10,1)   | YES  |     | NULL              |  -- minutes of stay
| stay_gt_m50              | decimal(10,1)   | YES  |     | NULL              |
| stay_gt_m60              | decimal(10,1)   | YES  |     | NULL              |
| stay_gt_m70              | decimal(10,1)   | YES  |     | NULL              |
| stay_gt_m80              | decimal(10,1)   | YES  |     | NULL              |
| stay_gt_m90              | decimal(10,1)   | YES  |     | NULL              |
| stay_gt_m100             | decimal(10,1)   | YES  |     | NULL              |
| stay_gt_m110             | decimal(10,1)   | YES  |     | NULL              |
| stay_gt_m120             | decimal(10,1)   | YES  |     | NULL              |
| source_count             | int             | NO   |     | 0                 |
| act_host                 | varchar(64)     | YES  |     | NULL              |
| remarks                  | varchar(255)    | YES  |     | NULL              |
| imputed_flag             | tinyint(1)      | NO   |     | 0                 |  -- 0=copied original, 1=interpolated
| imputation_tier          | tinyint         | NO   |     | 0                 |  -- source-quality tier; Product A publishes rows <= SOURCE_MAX_IMPUTATION_TIER
| imputation_method        | varchar(32)     | YES  |     | NULL              |  -- e.g. 'linear'
| gap_minutes_at_imputation| int             | YES  |     | NULL              |
| imputation_left_anchor   | varchar(20)     | YES  |     | NULL              |
| imputation_right_anchor  | varchar(20)     | YES  |     | NULL              |
| aggregated_at            | timestamp       | NO   |     | CURRENT_TIMESTAMP |
+--------------------------+-----------------+------+-----+-------------------+
```

Sample row, truncated:

```text
id=1, startdate='20260315_1551', group_place_id='sendai2023.10',
chip_id='30EDA00AFBCC', device_type='M5Stack', interval_min=1,
flow_gt_m60=6, flow_gt_m80=237, flow_gt_m120=430,
stay_gt_m60=0.2, stay_gt_m80=40.9, stay_gt_m120=...
```

## Product B -- `direction_metrics2_per_place2_agg`

```sql
mysql> desc bleData2025d.direction_metrics2_per_place2_agg;
+---------------------+-----------------+------+-----+-------------------+
| Field               | Type            | Null | Key | Default           |
+---------------------+-----------------+------+-----+-------------------+
| id                  | bigint unsigned | NO   | PRI | auto_increment    |
| startdate           | varchar(20)     | NO   | MUL | NULL              |  -- 'YYYYMMDD_HHMM'
| from_group_place_id | varchar(64)     | NO   | MUL | NULL              |
| from_chip_id        | varchar(64)     | NO   |     | NULL              |
| from_device_type    | varchar(32)     | NO   |     | NULL              |
| to_group_place_id   | varchar(64)     | NO   | MUL | NULL              |
| to_chip_id          | varchar(64)     | NO   |     | NULL              |
| to_device_type      | varchar(32)     | NO   |     | NULL              |
| interval_min        | int             | NO   |     | NULL              |  -- 1, 5, or 60
| recorded_at         | datetime        | NO   | MUL | NULL              |
| count               | int             | NO   |     | NULL              |
| source_count        | int             | NO   |     | NULL              |
| act_host            | varchar(64)     | YES  |     | NULL              |
| remarks             | varchar(255)    | YES  |     | NULL              |
| aggregated_at       | timestamp       | NO   |     | CURRENT_TIMESTAMP |
+---------------------+-----------------+------+-----+-------------------+
```

Sample row, truncated:

```text
startdate='20260510_0000', from_group_place_id='sendai2023.1',
to_group_place_id='sendai2023.2', from_device_type='M5Stack',
to_device_type='M5Stack', interval_min=1, count=67
```
