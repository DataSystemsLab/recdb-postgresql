#!/usr/bin/perl
use strict;
use warnings;

print "Rebuilding PostgreSQL completely.\n";
print "Configuring PostgreSQL.\n";
system "./configure", "--prefix=@ARGV", "--enable-debug";
my $rv = $? >> 8;
if ($rv > 0) {
	print "Configuration error.\n";
	exit 1;
}

print "Compiling PostgreSQL.\n";
my @makepg = `make`;
$rv = $? >> 8;
if ($rv > 0) {
	print @makepg;
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
	print @installpg;
	print "Installation error.\n";
	exit 1;
} else {
	my $install_length = (scalar @installpg);
	print $installpg[($install_length - 1)];
}

# Writing out install path for later reference
open FILE, ">", "install.properties" or die $!;
print FILE $ARGV[0];
close FILE or die $!;
