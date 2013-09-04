#!/usr/bin/perl
use strict;
use warnings;

open FILE, "<", "install.properties" or die $!;
my @path = <FILE>;
close FILE or die $!;
chomp(@path);

print "Starting PostgreSQL frontend.\n";
my $arg_length = (scalar @ARGV);
if ($arg_length >= 3) {
	system "$path[0]/bin/createdb", "-h", "$ARGV[1]", "$ARGV[0]";
	system "$path[0]/bin/psql", "-h", "$ARGV[1]", "$ARGV[0]";
} else {
	system "$path[0]/bin/createdb", "-h", "localhost", "$ARGV[0]";
	system "$path[0]/bin/psql", "-h", "localhost", "$ARGV[0]";
}
