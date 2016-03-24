import os
import pandas as pd
import sqlalchemy as sa

CREATE_TABLE_TEMPLATE_D = """\
DROP TABLE IF EXISTS az_dream_2015_features.{d_table_name};

CREATE TABLE az_dream_2015_features.{d_table_name} AS
SELECT
d2t.drug d,
{feature_columns_string}
FROM az_dream_2015.{d2t_table_name} d2t
{left} JOIN az_dream_2015_features.{g_table_name} t ON (t.g = d2t.hgnc_name)
GROUP BY d2t.drug {cell_line};

ALTER TABLE az_dream_2015_features.{d_table_name}
MODIFY d VARCHAR(255);

CREATE INDEX a ON az_dream_2015_features.{d_table_name} (d);
"""

CREATE_TABLE_TEMPLATE_DC = """\
DROP INDEX a ON az_dream_2015_features.{d_table_name};

ALTER TABLE az_dream_2015_features.{d_table_name}
MODIFY c VARCHAR(255);

CREATE INDEX a ON az_dream_2015_features.{d_table_name} (d, c);
CREATE INDEX b ON az_dream_2015_features.{d_table_name} (c, d);
"""

CREATE_TABLE_TEMPLATE_DD = """\
DROP TABLE IF EXISTS az_dream_2015_features.{d_table_name};

CREATE TABLE az_dream_2015_features.{d_table_name} AS
SELECT
d2t_1.drug d_1,
d2t_2.drug d_2,
{feature_columns_string}
FROM az_dream_2015.{d2t_table_name} d2t_1
JOIN az_dream_2015.{d2t_table_name} d2t_2
{left} JOIN az_dream_2015_features.{g_table_name} t ON (
    d2t_1.hgnc_name = t.g_1 AND d2t_2.hgnc_name = t.g_2)
GROUP BY d2t_1.drug, d2t_2.drug {cell_line};

ALTER TABLE az_dream_2015_features.{d_table_name}
MODIFY d_1 VARCHAR(255),
MODIFY d_2 VARCHAR(255);

CREATE INDEX a ON az_dream_2015_features.{d_table_name} (d_1, d_2);
"""

CREATE_TABLE_TEMPLATE_DDC = """\
ALTER TABLE az_dream_2015_features.{d_table_name}
DROP INDEX a,
MODIFY c VARCHAR(255);

CREATE INDEX a ON az_dream_2015_features.{d_table_name} (d_1, d_2, c);
CREATE INDEX b ON az_dream_2015_features.{d_table_name} (c, d_1, d_2);
"""


def g2d(table_name,
        null_default=None,
        connection_string=os.environ['BIODB_CONNECTION_STR'] + "/az_dream_2015_features"):
    """
    """
    # Get the columns for the table you are converting
    engine = sa.create_engine(connection_string)
    df = pd.read_sql_query("select * from {} limit 0".format(table_name), engine)
    #
    for d2t_source in ['', '_stitch']:
        drug_pair = False
        cell_line = False
        feature_columns = []
        for column in df.columns:
            if column == 'g':
                drug_pair = False
                # feature_columns.append("t.{0} {0}".format(column))
            elif column in ['g_1', 'g_2']:
                drug_pair = True
                # feature_columns.append("t.{0} {0}".format(column))
            elif column == 'c':
                cell_line = True
                feature_columns.append("t.{0} {0}".format(column))
            elif any(column.endswith(s) for s in ['_mean', '_std']):
                if null_default is None:
                    feature_columns.append(
                        "avg(t.{0}) {0}{1}_mean".format(column, d2t_source))
                else:
                    feature_columns.append(
                        "avg(ifnull(t.{0}, {2})) {0}{1}_mean"
                        .format(column, d2t_source, null_default))
            elif any(column.endswith(s) for s in ['_max']):
                if null_default is None:
                    feature_columns.append(
                        "max(t.{0}) {0}{1}_max".format(column, d2t_source))
                else:
                    feature_columns.append(
                        "ifnull(max(t.{0}), {2}) {0}{1}_max"
                        .format(column, d2t_source, null_default))
            elif any(column.endswith(s) for s in ['_min']):
                if null_default is None:
                    feature_columns.append(
                        "min(t.{0}) {0}{1}_min".format(column, d2t_source))
                else:
                    feature_columns.append(
                        "ifnull(min(t.{0}), {2}) {0}{1}_min"
                        .format(column, d2t_source, null_default))
            else:
                print("Unknown ending for column: {}. Assuming mean...".format(column))
                feature_columns.append("avg(t.{0}) {0}{1}_mean".format(column, d2t_source))

        feature_columns_string = ",\n".join(feature_columns)

        if not drug_pair:
            d_table_name = table_name.replace('_gbg', '_gbd') + d2t_source
            create_table_template = (
                CREATE_TABLE_TEMPLATE_D +
                (CREATE_TABLE_TEMPLATE_DC if cell_line else '')
            )
        else:
            d_table_name = table_name.replace('_gbgg', '_gbdd') + d2t_source
            create_table_template = (
                CREATE_TABLE_TEMPLATE_DD +
                (CREATE_TABLE_TEMPLATE_DDC if cell_line else '')
            )
        #
        if table_name == d_table_name:
            raise Exception(
                "The old and new tables must have different names!\n"
                "table_name: '{}'\n"
                "d_table_name: '{}'"
                .format(table_name, d_table_name)
            )
        table_options = dict(
            g_table_name=table_name,
            d_table_name=d_table_name,
            d2t_table_name='drug_to_hgnc_target' + d2t_source,
            feature_columns_string=feature_columns_string,
            cell_line=', t.c' if cell_line else '',
            left='LEFT' if not cell_line else '',
        )
        create_table_command = create_table_template.format(**table_options).strip('; \n')
        for command in create_table_command.split(';'):
            command = command.strip()
            print(command + ';\n')
            engine.execute(command)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--table-name', type=str)
    args = parser.parse_args()
    g2d(args.table_name)
