import subprocess
import shlex

from datapkg import csv2sql


# %%
def run_in_terminal(path, connection_string):
    system_command = (
        'python /home/kimlab1/strokach/working/csv2sql/csv2sql/csv2sql.py \\'
        '--file {} --db {} --debug'
        .format(path, connection_string)
    )
    sp = subprocess.run(
        shlex.split(system_command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(sp.stdout.decode())
    print(sp.stderr.decode())
    return sp


# %% Small .tsv file
path = (
    '/home/kimlab1/strokach/databases/'
    'cosmic/grch38/cell_lines/v75/CosmicCellLineProject.tsv.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
# csv2sql.main(path, connection_string)
x = run_in_terminal(path, connection_string)


# %% Large .tsv file
path = (
    '/home/kimlab1/strokach/databases/'
    'cosmic/grch38/cell_lines/v75/CosmicCellLineProject.tsv.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
csv2sql.main(path, connection_string)


# %% VCF files
path = (
    '/home/kimlab1/strokach/databases/'
    'cosmic/grch38/cosmic/v75/VCF/CosmicNonCodingVariants.vcf.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
csv2sql.main(path, connection_string)


# %% 1
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/wgs/v75/CosmicWGS_StrucExport.tsv.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
csv2sql.main(path, connection_string, sep='\t', skiprows=1)


# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/wgs/v75/CosmicWGS_GeneExpression.tsv.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
csv2sql.main(path, connection_string)


# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/cell_lines/v75/VCF/CellLinesCodingMuts.vcf.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
csv2sql.main(path, connection_string)


# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/cell_lines/v75/VCF/CellLinesNonCodingVariants.vcf.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
csv2sql.main(path, connection_string)


# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/cosmic/v75/VCF/CosmicCodingMuts.vcf.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
csv2sql.main(path, connection_string)


# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/wgs/v75/VCF/WGSCodingMuts.vcf.gz'
)
connection_string = 'mysql://root:@localhost:3306/cosmic'
csv2sql.main(path, connection_string)


# %% Harmonizome files
path = (
    '/home/kimlab1/strokach/databases/'
    'cosmic/grch38/cosmic/v75/VCF/CosmicNonCodingVariants.vcf.gz'
)
connection_string = 'mysql://root:@localhost:3306/harmonizome'
csv2sql.main(path, connection_string)


# %%
df, dtypes = csv2sql.get_dtypes_for_file(path, **varargs)
csv2sql.create_db_table(table_name, df, dtypes)


# %% MySQL does not ignore the header column by default
mysql_skiprows = varargs.get('skiprows', 0) + 1
csv2sql.load_compressed_file_to_database(
    path, table_name, skiprows=mysql_skiprows, na_values=varargs.get('na_values')
)
