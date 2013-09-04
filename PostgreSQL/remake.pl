#!/usr/bin/perl
use strict;
use warnings;

#open FILE, "<", "install.properties" or die $!;
#my @lines = <FILE>;
#close FILE or die $!;
#print "$lines[0]";

print "Rebuilding PostgreSQL.\n";
print "Compiling PostgreSQL.\n";
my @makepg = `make`;
my $rv = $? >> 8;
if ($rv > 0) {
	print "Compilation error.\n";
	exit 1;
} else {
	my $make_length = (scalar @makepg);
	print $makepg[($make_length - 1)];
}

print "Installing PostgreSQL.\n";
my @installpg = `make install`;
$rv = $? >> 8;
if ($rv > 0) {
	print "Installation error.\n";
	exit 1;
} else {
	my $install_length = (scalar @installpg);
	print $installpg[($install_length - 1)];
}
