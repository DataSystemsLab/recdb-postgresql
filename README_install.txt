Installation Script Instructions
--------------------------------

Once you've synced with GitHub, the folder should contain the source code for PostgreSQL, as well as several Perl scripts and several different .jar files.

1. Run the installation script install.pl.
Usage: perl install.pl [abs_path]

[abs_path] is the absolute path to the directory where you want PostgreSQL installed. The directory should exist before running this script.

2. Run the database server script pgstart.pl.
Usage: perl pgstart.pl

The install.pl script stores the install path, so there shouldn't be any need to specify it.

3. In a second terminal, run the database interaction script dbstart.pl.
Usage: perl dbstart.pl [db_name] [server_host]

[db_name] is the name of the database that you intend to use.
[server_host] is the address of the host server running the PostgreSQL backend. If this option is not specified, the script assumes it to be "localhost".

--------------------------------

If you need to rebuild PostgreSQL, there are two options.

If you have not modified the grammar, you can do a quick rebuild with remake.pl.
Usage: perl remake.pl

If you have modified the grammar, you will need to do a longer rebuild with remakefull.pl.
Usage: perl remakefull.pl [abs_path]
[abs_path] is the absolute path to the directory where you want PostgreSQL installed. The directory should exist before running this script.

If you ever want to eliminate the current database and all associated recommender files (namely recommenders.properties and ratingtables.properties), use the clean.pl script.
Usage: perl clean.pl [db_name] [server_host]