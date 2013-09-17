#!/usr/bin/perl

# Start RecDB Server (Backend)

use strict;
use warnings;

open FILE, "<", "install.properties" or die $!;
my @path = <FILE>;
close FILE or die $!;
chomp (@path);

print "Starting PostgreSQL backend.\n";
system "$path[0]/bin/postgres", "-D", "data";
