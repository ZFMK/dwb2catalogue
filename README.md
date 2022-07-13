# Import specimen and taxa from DiversityWorkbench databases to Collection Catalogue

Documentation on importing Specimens and Taxa from DiversityWorkbench databases into the collections catalogue. The import requires 4 steps:


1. Merge taxa from different DiversityTaxonNames databases into a common tree for the MySQL database of the collection catalogue.
2. Import specimen data from different DiversityCollection databases into the MySQL database of the collection catalogue. Check the taxon names applied to the specimens against the entries in the taxon tree and sort out specimens with unknown taxon names. These 2 steps are now done by the [dwb2catalogue](https://github.com/ZFMK/dwb2catalogue) script.
3. [dwb2catalogue](https://github.com/ZFMK/dwb2catalogue) creates a temporary database with the transfered data. When the transfer has been successfull, the temporary database is copied into the production database.
4. Index the data imported to the with a solr indexer. The configuration of the solr service is given in [collsolr](https://github.com/ZFMK/collsolr). [dwb2catalogue](https://github.com/ZFMK/dwb2catalogue) calls the solr service to create a new index.


## System requirements:

    apt-get install software-properties-common \
    python3-dev \
    python3-setuptools \
    python3-pip \
    unixodbc unixodbc-dev \
    tdsodbc


## Prerequisites

- One or more DiversityTaxonNames databases are available from which at least one contains a taxonomy that is rooted down to the Animal regnum (optionally you can use the GBIF taxonomy imported into a DiversityTaxonNames instance as described [here](https://github.com/ZFMK/gbif2mysql) and [here](https://github.com/ZFMK/gbif2tnt)


## Installation of required scripts

### Create Python Virtual Environment:

    python3 -m venv dwb2catalogue_venve
    cd dwb2catalogue_venv


Activate virtual environment:

    source bin/activate

Upgrade pip and setuptools

    pip install -U pip
    pip install -U setuptools


#### Clone sync_dwb_webportal from github.com: 

    git clone https://github.com/ZFMK/dwb2catalogue.git

#### Install the sync_dwb_webportal script

    cd dwb2catalogue
    pip install -r requierements.txt
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
    connection = Server=dwb.my_server1.de;DataBase=DiversityCollection_XY;UID=username;PWD=*****;Port=1433
    project_id = 600-19999
    analysis_id_tools = 95|110
    analysis_id_methods = 161
    respect_withhold = true

    [data_source_gbol]
    connection = Server=dwb.my_server1.de;DataBase=DiversityCollection_A;UID=username;PWD=******;Port=1433
    project_id = 20000,203,405
    analysis_id_tools = 95|110
    analysis_id_methods = 161
    respect_withhold = false


You also need to add or remove sections for the `DiversityTaxonNames` databases. Each section name must start with `tnt_` in the name to be recognized by the script. Set the projectids as komma separated values to define the projects from which the taxa should be read. 

Examples:

    [data_source_zfmk]
    connection = Server=dwb.my_server1.de;DataBase=DiversityCollection_XY;UID=username;PWD=*****;Port=1433
    project_id = 600-19999
    analysis_id_tools = 95|110
    analysis_id_methods = 161
    respect_withhold = true

    [data_source_gbol]
    connection = Server=dwb.my_server1.de;DataBase=DiversityCollection_A;UID=username;PWD=******;Port=1433
    project_id = 20000,203,405
    analysis_id_tools = 95|110
    analysis_id_methods = 161
    respect_withhold = false






#### Running the sync_dwb_webportal script

run the script in activated environment

    python Transfer.py

This script takes about 1.5 hours on a machine with MySQL database on SSD but old AMD FX 6300 CPU. Progress is logged to `sync_dwb2portal.log`







