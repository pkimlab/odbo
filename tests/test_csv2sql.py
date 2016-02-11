# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 17:42:40 2016

@author: strokach
"""
import subprocess
import shlex
from importlib import reload

from common import csvsql2
reload(csvsql2)

def run_in_terminal(path, connection_string):
    system_command = (
        'python /home/kimlab1/strokach/working/common/common/csvsql2.py \\'
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
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
# csvsql2.main(path, connection_string)
x = run_in_terminal(path, connection_string)


# %% Large .tsv file
path = (
    '/home/kimlab1/strokach/databases/'
    'cosmic/grch38/cell_lines/v75/CosmicCellLineProject.tsv.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
csvsql2.main(path, connection_string)


# %% VCF files
path = (
    '/home/kimlab1/strokach/databases/'
    'cosmic/grch38/cosmic/v75/VCF/CosmicNonCodingVariants.vcf.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
csvsql2.main(path, connection_string)








# %% 1
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/wgs/v75/CosmicWGS_StrucExport.tsv.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
csvsql2.main(path, connection_string, sep='\t', skiprows=1)




# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/wgs/v75/CosmicWGS_GeneExpression.tsv.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
csvsql2.main(path, connection_string)






# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/cell_lines/v75/VCF/CellLinesCodingMuts.vcf.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
csvsql2.main(path, connection_string)


# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/cell_lines/v75/VCF/CellLinesNonCodingVariants.vcf.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
csvsql2.main(path, connection_string)


# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/cosmic/v75/VCF/CosmicCodingMuts.vcf.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
csvsql2.main(path, connection_string)


# %%
path = (
    '/home/kimlab1/strokach/databases/cosmic'
    '/grch38/wgs/v75/VCF/WGSCodingMuts.vcf.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/cosmic'
csvsql2.main(path, connection_string)
















# %% Harmonizome files
path = (
    '/home/kimlab1/strokach/databases/'
    'cosmic/grch38/cosmic/v75/VCF/CosmicNonCodingVariants.vcf.gz'
)
connection_string = 'mysql://strokach:@192.168.6.19:3306/harmonizome'
csvsql2.main(path, connection_string)


# %%



# %%



# %%



# %%
df, dtypes = csvsql2.get_dtypes_for_file(path, **varargs)
csvsql2.create_db_table(table_name, df, dtypes)


# %% MySQL does not ignore the header column by default
mysql_skiprows = varargs.get('skiprows', 0) + 1
csvsql2.load_compressed_file_to_database(
    path, table_name, skiprows=mysql_skiprows, na_values=varargs.get('na_values')
)
