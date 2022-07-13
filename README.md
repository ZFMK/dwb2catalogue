# Import specimen and taxa from DiversityWorkbench databases to Collection Catalogue

Documentation on importing Specimens and Taxa from DiversityWorkbench databases into the collections catalogue. The import requires 3 steps:


1. Merge taxa from different DiversityTaxonNames databases into a common tree for the MySQL database of the collection catalogue.
2. Import specimen data from different DiversityCollection databases into the MySQL database of the collection catalogue. Check the taxon names applied to the specimens against the entries in the taxon tree and sort out specimens with unknown taxon names. These 2 steps are now done the [sync_dwb_webportal](https://github.com/ZFMK/dwb2portal) script.
3. Index the data imported to the with a solr indexer. The configuration of the solr service is given in [collsolr](https://github.com/ZFMK/collsolr) .


## Prerequisites

- One or more DiversityTaxonNames databases are available from which at least one contains a taxonomy that is rooted down to the Animal regnum (optionally you can use the GBIF taxonomy imported into a DiversityTaxonNames instance as described [here](https://github.com/ZFMK/gbif2mysql) and [here](https://github.com/ZFMK/gbif2tnt)


## Installation of required scripts

### Create Python Virtual Environment:

    python3 -m venv dwb2portal_venv
    cd dwb2portal_venv


Activate virtual environment:

    source bin/activate

Upgrade pip and setuptools

    python -m pip install -U pip
    pip install --upgrade pip setuptools




#### Clone sync_dwb_webportal from github.com: 

    git clone https://github.com/ZFMK/dwb2catalogue.git

#### Install the sync_dwb_webportal script

    cd dwb2catalogue
    python setup.py develop


#### Configure the sync_dwb_webportal script

    cp config.template.ini config.ini

Insert the needed database connection values into `config.ini`.

First insert the credentials and connection parameters for the MySQL database of the webportal in section [zfmk_coll].


    [zfmk_coll]
    host = 
    user = 
    passwd = 
    db = webportal_db
    charset = utf8


Then edit, add or remove sections for the `DiversityCollection` databases. Each section name must start with `data_source_` in the name to be recognized by the script. Set the projectids as komma separated values to define the projects from which the specimens should be read. The respect_withhold entry defines whether withhold-flags in the database tables should be followed (`true`) or ignored (`false`). Following the withhold flags means, only data that are not flagged for withhold are transfered to the portal database.

Examples:

    [data_source_zfmk]
    connection = DSN=DWB@Server1;DataBase=DiversityCollection_XY;UID=username;PWD=*****
    project_id = 600-19999
    analysis_id_tools = 95|110
    analysis_id_methods = 161
    respect_withhold = true

    [data_source_gbol]
    connection = DSN=DWB@Server2;DataBase=DiversityCollection_A;UID=username;PWD=******
    project_id = 20000,203,405
    analysis_id_tools = 95|110
    analysis_id_methods = 161
    respect_withhold = false

The section names in brackets must match with entries in file `/etc/odbc.ini` (see [below](https://github.com/ZFMK/dwb2catalogue/blob/main/README.md#freetds))


#### Running the sync_dwb_webportal script

run the script in activated environment

    python Transfer.py

This script takes about 1.5 hours on a machine with MySQL database on SSD but old AMD FX 6300 CPU. Progress is logged to `syn_dwb2portal.log`







