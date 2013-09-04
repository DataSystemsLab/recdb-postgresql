#!/usr/bin/perl
use strict;
use warnings;

open FILE, "<", "install.properties" or die $!;
my @path = <FILE>;
close FILE or die $!;
chomp (@path);

print "Removing database $ARGV[0].\n";
my $arg_length = (scalar @ARGV);
if ($arg_length >= 2) {
	system "$path[0]/bin/dropdb", "-h", "$ARGV[1]", "$ARGV[0]";
} else {
	system "$path[0]/bin/dropdb", "-h", "localhost", "$ARGV[0]";
}
system "rm", "$path[0]/data/ratingtables.properties";
system "rm", "$path[0]/data/recommenders.properties";
