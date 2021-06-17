# Import from DiversityWorkbench to Collection Catalogue

Documentation on importing Specimens and Taxa from DiversityWorkbench databases into the collections catalogue. The import requires 3 steps:


1. Merge taxa from different DiversityTaxonNames databases into a common tree for the MySQL database of the collection catalogue. This is done by the [tnt_taxa_merger](https://gitlab.leibniz-lib.de/zfmk-collections/tnt_taxa_merger) script.
2. Import specimen data from different DiversityCollection databases into the MySQL database of the collection catalogue. Check the taxon names applied to the specimens against the entries in the taxon tree and sort out specimens with unknown taxon names. This is done by the [sync_dwb_webportal](https://gitlab.leibniz-lib.de/zfmk-collections/sync_dwb_webportal) script.
3. Index the data imported to the with a solr indexer. The configuration of the solr service is given in collsolr.


## Prerequisites

- FreeTDS installed as described [here](#freetds) 
- One or more DiversityTaxonNames databases are available from which at least one contains a taxonomy that is rooted down to the Animal regnum (optionally you can use the GBIF taxonomy imported into a DiversityTaxonNames instance as described [here](https://gitlab.leibniz-lib.de/zfmk-collections/gbif2taxonnames))


## Installation of required scripts

### Create Python Virtual Environment:

    python3 -m venv dwb2portal_venv
    cd dwb2portal_venv


Activate virtual environment:

    source bin/activate

Upgrade pip and setuptools

    python -m pip install -U pip
    pip install --upgrade pip setuptools




#### Clone sync_dwb_webportal from gitlab: 

    git clone https://gitlab.leibniz-lib.de/zfmk-collections/sync_dwb_webportal.git

**Very important: change to branch `to_github`**

    git checkout to_github


#### Install the sync_dwb_webportal script

    cd sync_dwb_webportal
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


#### Running the sync_dwb_webportal script

run the script in activated environment

    python Transfer.py

This script takes about 1.5 hours on a machine with MySQL database on SSD but old AMD FX 6300 CPU. Progress is logged to `syn_dwb2portal.log`



----

## FreeTDS

Download and install FreeTDS driver for SQL-Server Database

    wget ftp://ftp.freetds.org/pub/freetds/stable/freetds-1.2.18.tar.gz
    tar -xf freetds-1.2.18.tar.gz
    cd freetds-1.2.18
    ./configure --prefix=/usr --sysconfdir=/etc --with-unixodbc=/usr --with-tdsver=7.2
    make
    sudo make install

Setup odbc-driver and config

Create file `tds.driver.template` with content:

    [FreeTDS]
    Description = v0.82 with protocol v8.0
    Driver = /usr/lib/libtdsodbc.so


Register driver

    sudo odbcinst -i -d -f tds.driver.template

Create entry in `/etc/odbc.ini` 

    [TaxonNames] 
    Driver=FreeTDS
    TDS_Version=7.2
    APP=Some meaningful appname
    Description=DWB SQL DWB Server
    Server=<some TaxonNames Server>
    Port=<port>
    Database=<a TaxonNames database>
    QuotedId=Yes
    AnsiNPW=Yes
    Mars_Connection=No
    Trusted_Connection=Yes
    client charset = UTF-8






