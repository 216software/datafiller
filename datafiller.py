#! /usr/bin/env python
#
# $Id: datafiller.py 268 2013-06-30 16:01:33Z fabien $
#
# TODO:
# - types: BIT/ARRAY? others?
# - disable/enable constraints? within a transaction?
#   not possible with PostgreSQL, must DROP/CREATE?
# - improve "parsing" regexpr, including some support for ALTER?
# - real parser? PLY? see http://nedbatchelder.com/text/python-parsers.html

VERSION = '1.1.2'

import re
Id='$Id: datafiller.py 268 2013-06-30 16:01:33Z fabien $'
revision, revdate = re.match(r'.*? (\d+) (\d{4}-\d\d-\d\d) ', Id).group(1, 2)
version = "{0} (r{1} on {2})".format(VERSION, revision, revdate)

# python 2/3 compatibility hacks -- this is tiring:-(
# - D.has_key(K) => K in D # better anyway
# - "..." % ... => "...".format(...) # advised, not necessary?
# - print ... => print(...) # ok
# - print => print('')      # hmmm...
# - xrange => range         # grrr...
# - casts: s[:l] => s[:int(l)], sn * fp => sn * int(fp) # this is a regression
# - StringIO: changed place   # should be transparent
import sys
if sys.version_info < (3,):
    # python 2
    from StringIO import StringIO
    range = xrange
else:
    # python 3
    from io import StringIO

# plain old embedded documentation... Yes, the perl thing;-)
# could use pandoc/markdown,but it seems that pandoc cannot display
# a manual page interactively as pod2usage, and as a haskell script
# it is not installed by default.
POD="""

=pod

=head1 NAME

B<datafiller.py> - generate random data from database schema extended with directives

=head1 SYNOPSIS

B<datafiller.py> [--help --man ...] [schema.sql ...] > data.sql

=head1 DESCRIPTION

This script generates random data from a database schema enriched with
simple directives in SQL comments to drive eleven data generators which
cover typical data types.
Reasonable defaults are provided, especially based on key and type constraints,
so that few directives should be necessary. The minimum setup is to specify the
relative size of tables with directive B<mult> so that the data generation can
be scaled.
Run with C<--test=comics> and look at the output for a didactic example.

=head1 OPTIONS

=over 4

=item C<--debug> or C<-D>

Set debug mode.
Default is no debug.

=item C<--drop>

Drop tables before recreating them.
This implies option C<--filter>, otherwise there would be no table to fill.

Default is not to.

=item C<--filter> of C<-f>

Work as a filter, i.e. send the schema input script to stdout and then
the generated data.
This is convenient to pipe the result of the script directly for
execution to the database command.

Default is to only ouput generated data.

=item C<--help> or C<-h>

Show basic help.

=item C<--man> or C<-m>

Show full man page based on POD. Yes, the perl thing:-)

=item C<--mangle>

Use random steps and shifts for integer generators.
This is useful for avoiding strong correlations between tuple keys.

Default is to use 1 for step and 0 for shift.
This can also be set with the B<mangle> directive at the schema level,
or overriden one way or the other with per-attribute with directives
B<mangle> or B<nomangle>, or with explicit B<step> and B<shift> directives.

=item C<--null RATE> or C<-n RATE>

Probability to generate a null value for nullable attributes.

Default is 0.01, which can be overriden by the B<null> directive at
the schema level, or per-attributes provided B<null> rate.

=item C<--offset OFFSET> or C<-O OFFSET>

Set default offset for integer generators on I<primary keys>.
This is useful to extend the already existing content of a database
for larger tests.

Default is 1, which can be overriden by the B<offset> directive at
the schema level, or per-attribute provided B<offset>.

=item C<--pod COMMAND>

Override pod conversion command used by option C<--man>.

Default is 'pod2usage -verbose 3'.

=item C<--seed SEED> or C<-S SEED>

Seed random generated with provided string.

Default uses OS supplied randomness or current time.

=item C<--size SIZE>

Set overall scaling. The size is combined with the B<mult> directive value
on a table to compute the actual number of tuples to generate in each table.

Default is 100, which can be overriden with the B<size> directive at the
schema level.

=item C<--target (postgresql|mysql)> or C<-t ...>

Target database engine. MySQL support is really experimental.

Default is to target PostgreSQL.

=item C<--test=(comics|pgbench|validate)> or C<--test='int:directives...'>

Output test data for B<comics> or B<pgbench> schemas (see L</EXAMPLE> below),
or the internal validation,
or run tests for bool, integer, float or blob generators with some directives.

Example: --test='bool:rate=0.3' may show I<True: 30.68%>,
stating the rate at which I<True> was actually seen during the test.

Option C<--test=...> sets C<--filter> automatically.

Default is to process argument files or standard input.

=item C<--transaction> or C<-T>

Use a global transaction.

Default is not to.

=item C<--tries=NUM>

How hard to try to satisfy a compound unique constraint before giving up
on a given tuple.

Default is 10.

=item C<--truncate>

Delete table contents before filling.

Default is not to.

=item C<--validate>

Shortcut for C<--test=validate --filter --transaction>.

To run the validation in a temporary schema C<df>:

  sh> datafiller.py --validate | psql

=item C<--version> or C<-v>

Show script version.

=back

=head1 ARGUMENTS

Files containing SQL schema definitions, or F<stdin> is processed if empty.

=head1 DIRECTIVES AND DATA GENERATORS

Directives drive the data sizes and the underlying data generators.
They must appear in SQL comments I<after> the object on which they apply,
although possibly on the same line, introduced by S<'-- df: '>.

  CREATE TABLE Stuff(         -- df: mult=2.0
    id SERIAL PRIMARY KEY,    -- df: step=19
    data TEXT UNIQUE NOT NULL -- df: prefix=st length=30 lenvar=3
  );

In the above example, with option C<--size=1000>, 2000 tuples
will be generated I<(2.0*1000)> with B<id> I<1+(i*19)%2000> and
unique text B<data> of length about 30+-3 prefixed with C<st>.
The sequence for B<id> will be restarted at I<2001>.

The default size is the number of tuples of the containing table.
This implies many collisions for a I<uniform> generator.

=head2 DATA GENERATORS

There are eleven data generators which are selected by the attribute type
or possibly directives. All generators are also subject to the B<null>
directive which drives the probability of a C<NULL> value.

=over 4

=item B<bool generator>

This generator is used for the boolean type.
It is subject to the B<rate> directive.

=item B<int generator>

This generator is used directly for integer types, and indirectly
by text, word and date generators.
Its internal working is subject to directives: B<gen>, B<size> (or B<mult>),
B<offset>, B<shift>, B<step>, B<mangle> and B<nomangle>.

=item B<float generator>

This generator is used for floating point types.
It does not support C<UNIQUE>, but uniqueness is very likely.
Its configuration relies on directives B<gen>, B<alpha> and B<beta>.

=item B<date generator>

This generator is used for the date type.
It uses an B<int generator> internally to drive its extent.
Its internal working is subject to directives B<start>, B<end> and B<prec>.

=item B<timestamp generator>

This generator is used for the timestamp type.
It is similar to the date generator but at a finer granularity.
The B<tz> directive allows to specify the target timezone.

=item B<interval generator>

This generator is used for the time interval type.
It uses the B<int generator> internally to drive its extent.
See also the B<unit> directive.

=item B<string generator>

This generator is used by default for text types.
This is a good generator for filling stuff without much ado.

It takes into account B<prefix>, B<length> and B<lenvar> directives.
The generated text is of length B<length> +- B<lenvar>.
For C<CHAR(n)> and C<VARCHAR(n)> text types, automatic defaults are set
for both B<length> and B<lenvar>.

=item B<chars generator>

This alternate generator for text types generates random string of characters.
It is triggered by the B<chars> directive.

In addition to the underlying B<int generator> which allows to select values,
another B<int generator> is used to build words from the provided list
of characters,

The B<cgen> directives is the name of a macro which specifies the
B<int generator> parameters for the random char selection.

It also takes into account the B<length> and B<lenvar> directives.

This generator does not support C<UNIQUE>.

=item B<word generator>

This alternate generator for text types is triggered by the B<word> directive.
It uses B<int generator> to select words from a list or a file.

This generator handles C<UNIQUE> if enough words are provided.

=item B<text generator>

This alternate generator for text types generates sentences of words
drawn from a list of words specified with directive B<word>.
It is triggered by the B<text> directive.

It also takes into account the B<length> and B<lenvar> directives which
handle the number of words to generate.

This generator does not support C<UNIQUE>, but uniqueness is very likely
for a text with a significant length drawn from a dictionnary.

=item B<blob generator>

This is for blob types, such as PostgreSQL's C<BYTEA>.

This generator does not support C<UNIQUE>.

=back

=head2 GLOBAL DIRECTIVES

A directive macro can be defined and then used later by inserting its name
between the introductory C<df> and the C<:>. The specified directives are
stored in the macro and can be reused later.
For instance, macros B<words>, B<mangle> B<cfr> and B<cen> can be defined as:

  --df words: word=/etc/dictionaries-common/words gen=power alpha=1.7
  --df mix: offset=10000 step=17 shift=3
  --df cfr: gen=scale alpha=6.7
  --df cen: gen=scale alpha=5.9

Then they can be used in any datafiller directive with B<use=...>:

  --df: use=words use=mix
  --df: use=mix

Or possibly for chars generators with B<cgen=...>:

  --df: cgen=cfr chars='esaitnru...'

There are four predefined macros:
B<cfr> and B<cen> define skewed integer generators with the above parameters.
B<french>, B<english> define chars generators which tries to mimic the
character frequency of these languages.

The B<size>, B<offset>, B<mangle>, B<null> and B<seed> directives
can be defined at the schema level to override from the SQL script
the default size multiplier, primary key offset, use of random shifts and
steps, null rate and seed.
However, they are ignored if the corresponding options are set.

=head2 TABLE DIRECTIVES

=over 4

=item B<mult=float>

Size multiplier for scaling, that is computing the number of tuples to
generate.

=item B<nogen>

Do not generate data for this table.

=item B<null>

Set defaut B<null> rate for this table.

=item B<size=int>

Use this size, so there is no scaling with the C<--size> option
and B<mult> directive.

=item B<skip=float>

Skip (that is generate but do not insert) some tuples with this probability.
Useful to create some holes in data. Tables with a non-zero B<skip> cannot be
referenced.

=back

=head2 ATTRIBUTE DIRECTIVES

=over 4

=item B<type=(int|float|bool|date|...)>

Force specified type generator regardless of attribute type.
The default is to select the generator based on the attribute type,
so this option should never be necessary.

=item B<gen=GENERATOR>

For integer or float type, use this underlying generator.

The generators for integers are:

=over 4

=item B<serial>

This is really a counter which generates distinct integers,
depending on B<offset>, B<shift> and B<step>.

=item B<uniform>

Generates uniform random number integers between B<offset> and B<offset+size-1>.
This is the default.

=item B<serand>

Generate integers based on B<serial> up to B<size>, then use B<uniform>.
Useful to fill foreign keys.

=item B<power> with parameter B<alpha> or B<rate>

Use probability to this B<alpha> power.
When B<rate> is specified, compute alpha so that value 0 is drawn
at the specified rate.
Uniform is similar to B<power> with B<alpha=1.0>, or I<B<rate>=1.0/size>
The higher B<alpha>, the more skewed towards I<0>.

Example distribution with C<--test='int:gen=power rate=0.3 size=10'>:

  value     0   1   2   3   4   5   6   7   8   9
  percent  30  13  10   9   8   7   6   6   5   5

=item B<scale> with parameter B<alpha> or B<rate>

Another form of skewing. The probability of increasing values drawn
is less steep at the beginning compared to B<power>, thus the probability
of values at the end is lower.

Example distribution with C<--test='int:gen=scale rate=0.3 size=10'>:

  value     0   1   2   3   4   5   6   7   8   9
  percent  30  19  12   9   7   6   5   4   3   2

=back

The random generators for floats are those provided by Python's C<random>:

=over 4

=item B<beta>

Beta distribution, B<alpha> and B<beta> must be >0.

=item B<exp>

Exponential distribution with mean 1.0 / B<alpha>

=item B<gamma>

Gamma distribution, B<alpha> and B<beta> must be >0.

=item B<gauss>

Gaussian distribution with mean B<alpha> and stdev B<beta>.

=item B<log>

Log normal distribution, see B<normal>.

=item B<norm>

Normal distribution with mean B<alpha> and stdev B<beta>.

=item B<pareto>

Pareto distribution with shape B<alpha>.

=item B<uniform>

Uniform distribution between B<alpha> and B<beta>.
This is default distribution.

=item B<vonmises>

Circular data distribution, with mean angle B<alpha> in radians
and concentration B<beta>.

=item B<weibull>

Weibull distribution with scale B<alpha> and shape B<beta>.

=back

=item B<chars='abcdefghijkl' cgen=macro>

The B<chars> directive triggers the B<chars generator> described above.
Directive B<chars> provides a list of characters which are used to build words.
The macro name specified in directive B<cgen> is used to setup the character
selection random generator.

For exemple:

  ...
  -- df skewed: gen=power rate=0.3
  , stuff TEXT -- df: gen=uniform chars='abcdef' size=23 cgen=skewed

The text is chosen uniformly in a list of 23 words, each word being
built from characters 'abcdef' with the I<skewed> generator described
in the corresponding macro definition on the line above.

=item B<mangle> or B<nomangle>

Whether to automatically choose random B<shift> and B<step> for
an integer generator, or not.

=item B<mult=float>

Use this multiplier to compute the generator B<size>.

=item B<nogen>

Do not generate data for this attribute, so it will get its default value.

=item B<null=float>

Probability of generating a null value for this attribute.
This applies to all generators.

=item B<offset=int shift=int step=int>

Various parameters for generated integers.
The generated integer is B<offset+(shift+step*i)%size>.
B<step> must not be a divider of B<size>, it is ignored and replaced
with 1 if so.

Defaults: offset is 1, shift is 0, step is 1.

=item B<prefix=st>

Prefix for string data.

=item B<length=int lenvar=int>

Length and length variation for generated characters of string data or
number of words of text data.

=item B<rate=float>

For the bool generator, rate of generating I<True> vs I<False>.
Must be in [0, 1]. Default is I<0.5>.

For the int generator, rate of generating value 0 for generators
B<power> and B<scale>.

=item B<seed=str>

Set default global seed from the schema level.
This can be overriden by option C<--seed>.
Default is to used the default random generator seed, usually
relying on OS supplied randomness or the current time.

=item B<size=int>

Number of underlying values to generate or draw from, depending on the
generator. For keys (primary, foreign, unique) , this is necessarily the
corresponding number of tuples.

=item B<start=date/time> , B<end=date/time>, B<prec=int>

For the B<date> and B<timestamp> generators,
issue from B<start> up to B<end> at precision B<prec>.
Precision is in days for dates and seconds for timestamp.
Default is to set B<end> to current date/time and B<prec> to
1 day for dates et 60 seconds for timestamps.
If both B<start> and B<end> are specified, the underlying size is
adjusted.

For example, to draw from about 100 years of dates ending on
January 19, 2038:
  -- df: end=2038-01-19 size=36525

=item B<text>

The B<text> directive triggers the B<text generator>.
This generator requires the list of words to draw from to be specified
with the B<word> directive.

=item B<unit>

The B<unit> directive specifies the unit of the generated intervals.
Possible values include B<s m h d mon y>. Default is B<s>, i.e. seconds.

=item B<word=file> or B<word=:list,of,words>

The B<word> directive triggers the B<word generator> described above.
Use provided word list or lines of file to generate data.
The default B<size> is the size of the word list.

If the file contents is ordered by word frequency, and the int generator is
skewed (see B<gen>), the first words can be made to occur more frequently.

=back

=head1 EXAMPLES

The first example is a didactic schema to illustrate directives.
The second example is taken from B<pgbench>.
As both schemas are embedded into this script, they can be invoked
directly with the C<--test> option:

  sh> datafiller.py --test=comics -T --size=10 | psql bench
  sh> datafiller.py --test=pgbench -T --size=10 | psql bench

=head2 COMICS SCHEMA

This schema models B<Comics> books written in a B<Language> and
published by a B<Publisher>. Each book can have several B<Author>s
through B<Written>. The B<Inventory> tells on which shelf books are
stored. Some of the books may be missing and a few may be available
twice or more.

{comics}

=head2 PGBENCH SCHEMA

This schema is taken from the TCP-B benchmark.
Each B<Branch> has B<Tellers> and B<Accounts> attached to it.
The B<History> records operations performed when the benchmark is run.

{pgbench}

The integer I<*balance> figures are generated with a skewed generator
defined in macro B<regress>. The negative B<offset> setting on I<abalance>
will help generate negative values, and the I<regress> skewed generator
will make small values more likely.

If this is put in a C<tpc-b.sql> file, then working test data can be
generated with:

  sh> datafiller.py -f -T --size=10 tpc-b.sql | psql bench

=head1 BUGS AND FEATURES

All software has bug, this is a software, hence it has bugs.

If you find one, please sent a report, or even better a patch that fixes it!

There is no SQL parser, table and attributes are analysed with basic
regular expressions. The C<ALTER> syntax is fully ignored.

Foreign keys cannot reference compound keys.

Inconsistent directives may be set.
Some directives may be ignored in some cases.

Handling of quoted identifiers is partial and may not work at all.

Beware that unique constraint checks for big data generation may require
a lot of memory.

=head1 LICENSE

=for html
<img src="http://www.gnu.org/graphics/gplv3-127x51.png"
alt="GNU GPLv3" align="right" />

Copyright 2013 Fabien Coelho <fabien at coelho dot net>

This is free software, both inexpensive and with sources.

The GNU General Public License v3 applies, see
L<http://www.gnu.org/copyleft/gpl.html> for details.

The summary is: you get as much as you paid for, and I am not responsible
for anything.

If you are happy with this software, feel free to send me a postcard saying so!
See my web page for current address L<http://www.coelho.net/>.

=head1 SEE ALSO

Relational data generation tools are often GUI or even Web applications,
possibly commercial. I did not find a simple filter-oriented tool driven
by directives, and I wanted to do something useful to play with python.

=over 4

=item L<http://en.wikipedia.org/wiki/Test_data_generation>

=item L<http://generatedata.com/> PHP/MySQL

=item L<http://www.databasetestdata.com/> generate one table

=item L<http://www.mobilefish.com/services/random_test_data_generator/random_test_data_generator.php>

=item L<http://www.sqledit.com/dg/> Win GUI/MS SQL Server, Oracle, DB2

=item L<http://sourceforge.net/projects/spawner/> Pascal, GUI for MySQL

=item L<http://sourceforge.net/projects/dbmonster/> 2005 - Java from XML
for PostgreSQL, MySQL, Oracle

=item L<http://sourceforge.net/projects/datagenerator/> 2006, alpha in Pascal, Win GUI

=item L<http://sourceforge.net/projects/dgmaster/> 2009, Java, GUI

=item L<http://www.gsapps.com/products/datagenerator/> GUI/...

=item L<http://rubyforge.org/projects/datagen>

=item L<http://msdn.microsoft.com/en-us/library/dd193262%28v=vs.100%29.asp>

=item L<http://stackoverflow.com/questions/3371503/sql-populate-table-with-random-data>

=item Perl (Data::Faker Data::Random), Ruby (Faker ffaker), Python random

=back

=head1 DOWNLOAD

This is F<{script}> version {version}.

Latest version and online documentation should be available from
L<http://www.coelho.net/datafiller.html>.

Download script at
L<https://www.cri.ensmp.fr/people/coelho/datafiller.py>.

History of versions:

=over 4

=item B<version {version}>

Improved and simplified code, better comments and validation.
Various hacks for python 2 & 3 compatibility.
Make validations stop on errors.
Check that B<lenvar> is less than B<length>.
Fixes for B<length> and B<lenvar> overriding in B<string generator>.

=item B<version 1.1.1 (r250 on 2013-06-29)>

Minor fix to the documentation.

=item B<version 1.1.0 (r248 on 2013-06-29)>

Improved documentation, code and comments.
Add C<--test> option for demonstration and checks,
including an embedded validation.
Add C<--validate> option as a shortcut for script validation.
Add B<seed>, B<skip>, B<mangle> and B<nomangle> directives.
Add B<null> directive on tables.
Add B<nogen> and B<type> directives on attributes.
Accept size 0.
Change B<alpha> behavior under B<gen=scale> so that the higher B<alpha>,
the more skewed towards 0.
Add alternative simpler B<rate> specification for B<scale> and B<power>
integer generators.
Deduce B<size> when both B<start> and B<end> are specified for the
date and timestamp generators.
Add B<tz> directive for the timestamp generator.
Add float, interval and blob generators.
Add some support for user-defined enum types.
Add C<--tries> option to control the effort to satisfy C<UNIQUE> constraints.
Add support for non integer foreign keys.
Remove B<square> and B<cube> integer generators.
Change macro definition syntax so as to be more intuitive.
Add C<-V> option for short version.
Some bug fixes.

=item B<version 1.0.0 (r128 on 2013-06-16)>

Initial distribution.

=back

"""

PGBENCH = """
  -- TPC-B example adapted from pgbench

  \\set ON_ERROR_STOP

  -- df regress: gen=power alpha=1.5
  -- df: size=1

  CREATE TABLE pgbench_branches( -- df: mult=1.0
    bid SERIAL PRIMARY KEY,
    bbalance INTEGER NOT NULL,   -- df: size=100000000 use=regress
    filler CHAR(88) NOT NULL
  );

  CREATE TABLE pgbench_tellers(  -- df: mult=10.0
    tid SERIAL PRIMARY KEY,
    bid INTEGER NOT NULL REFERENCES pgbench_branches,
    tbalance INTEGER NOT NULL,   -- df: size=100000 use=regress
    filler CHAR(84) NOT NULL
  );

  CREATE TABLE pgbench_accounts( -- df: mult=100000.0
    aid BIGSERIAL PRIMARY KEY,
    bid INTEGER NOT NULL REFERENCES pgbench_branches,
    abalance INTEGER NOT NULL,   -- df: offset=-1000 size=100000 use=regress
    filler CHAR(84) NOT NULL
  );

  CREATE TABLE pgbench_history(  -- df: nogen
    tid INTEGER NOT NULL REFERENCES pgbench_tellers,
    bid INTEGER NOT NULL REFERENCES pgbench_branches,
    aid BIGINT NOT NULL REFERENCES pgbench_accounts,
    delta INTEGER NOT NULL,
    mtime TIMESTAMP NOT NULL,
    filler CHAR(22)
    -- UNIQUE (tid, bid, aid, mtime)
  );
"""

# embedded PostgreSQL validation
VALIDATE = """
\\set ON_ERROR_STOP

-- df: size=2000 null=0.0

CREATE SCHEMA df;

CREATE TYPE df.color AS ENUM ('red','blue','green');

CREATE TABLE df.Stuff( -- df: mult=1.0
  id SERIAL PRIMARY KEY
  -- integer
, i0 INTEGER CHECK(i0 IS NULL) -- df: null=1.0 size=1
, i1 INTEGER CHECK(i1 IS NOT NULL AND i1=1) -- df: null=0.0 size=1
, i2 INTEGER NOT NULL CHECK(i2 BETWEEN 1 AND 6) --df: size=5
, i3 INTEGER UNIQUE -- df: offset=1000000
, i4 INTEGER CHECK(i2 BETWEEN 1 AND 6) -- df: gen=power rate=0.7 size=5
, i5 INTEGER CHECK(i2 BETWEEN 1 AND 6) -- df: gen=scale rate=0.7 size=5
, i6 INT8  -- df: size=1800000000000000000 offset=-900000000000000000
, i7 INT4  -- df: size=4000000000 offset=-2000000000
, i8 INT2  -- df: size=65000 offset=-32500
  -- boolean
, b0 BOOLEAN NOT NULL
, b1 BOOLEAN -- df: null=0.5
, b2 BOOLEAN NOT NULL -- df: rate=0.7
  -- float
, f0 REAL NOT NULL CHECK (f0 >= 0.0 AND f0 < 1.0)
, f1 DOUBLE PRECISION -- df: gen=gauss alpha=5.0 beta=2.0
, f2 DOUBLE PRECISION CHECK(f2 >= -10.0 AND f2 < 10.0)
    -- df: gen=uniform alpha=-10.0 beta=10.0
, f3 DOUBLE PRECISION -- df: gen=beta alpha=1.0 beta=2.0
, f4 DOUBLE PRECISION -- df: gen=exp alpha=0.1
, f5 DOUBLE PRECISION -- df: gen=gamma alpha=1.0 beta=2.0
, f6 DOUBLE PRECISION -- df: gen=log alpha=1.0 beta=2.0
, f7 DOUBLE PRECISION -- df: gen=norm alpha=20.0 beta=0.5
, f8 DOUBLE PRECISION -- df: gen=pareto alpha=1.0
, f9 DOUBLE PRECISION -- df: gen=vonmises alpha=1.0 beta=2.0
, fa DOUBLE PRECISION -- df: gen=weibull alpha=1.0 beta=2.0
, fb NUMERIC(2,1) CHECK(fb BETWEEN 0.0 AND 9.9)
    -- df: gen=uniform alpha=0.0 beta=9.9
, fc DECIMAL(5,2) CHECK(fc BETWEEN 100.00 AND 999.99)
    -- df: gen=uniform alpha=100.0 beta=999.99
  -- date
, d0 DATE NOT NULL
       CHECK(d0 <= CURRENT_DATE AND d0 >= CURRENT_DATE - 1) -- df: size=2
, d1 DATE NOT NULL
       CHECK(d1 = DATE '2038-01-19') -- df: start=2038-01-19 end=2038-01-19
, d2 DATE NOT NULL
       CHECK(d2 = DATE '2038-01-19' OR d2 = DATE '2038-01-20')
       -- df: start=2038-01-19 size=2
, d3 DATE NOT NULL
       CHECK(d3 = DATE '2038-01-18' OR d3 = DATE '2038-01-19')
       -- df: end=2038-01-19 size=2
, d4 DATE NOT NULL
       CHECK(d4 = DATE '2013-06-01' OR d4 = DATE '2013-06-08')
       -- df: start=2013-06-01 end=2013-06-08 prec=7
, d5 DATE NOT NULL
       CHECK(d5 = DATE '2013-06-01' OR d5 = DATE '2013-06-08')
       -- df: start=2013-06-01 end=2013-06-14 prec=7
  -- timestamp
, t0 TIMESTAMP NOT NULL
          CHECK(t0 = TIMESTAMP '2013-06-01 00:00:05' OR
                t0 = TIMESTAMP '2013-06-01 00:01:05')
          -- df: start='2013-06-01 00:00:05' end='2013-06-01 00:01:05'
, t1 TIMESTAMP NOT NULL
          CHECK(t1 = TIMESTAMP '2013-06-01 00:02:00' OR
                t1 = TIMESTAMP '2013-06-01 00:02:05')
          -- df: start='2013-06-01 00:02:00' end='2013-06-01 00:02:05' prec=5
, t2 TIMESTAMP NOT NULL
          CHECK(t2 >= TIMESTAMP '2013-06-01 01:00:00' AND
                t2 <= TIMESTAMP '2013-06-01 02:00:00')
          -- df: start='2013-06-01 01:00:00' size=30 prec=120
, t3 TIMESTAMP WITH TIME ZONE NOT NULL
          CHECK(t3 = TIMESTAMP '2013-06-22 09:17:54 CEST')
          -- df: start='2013-06-22 07:17:54' size=1 tz='UTC'
  -- interval
, v0 INTERVAL NOT NULL CHECK(v0 BETWEEN '1 s' AND '1 m')
     -- df: size=59 offset=1 unit='s'
, v1 INTERVAL NOT NULL CHECK(v1 BETWEEN '1 m' AND '1 h')
     -- df: size=59 offset=1 unit='m'
, v2 INTERVAL NOT NULL CHECK(v2 BETWEEN '1 h' AND '1 d')
     -- df: size=23 offset=1 unit='h'
, v3 INTERVAL NOT NULL CHECK(v3 BETWEEN '1 d' AND '1 mon')
     -- df: size=29 offset=1 unit='d'
, v4 INTERVAL NOT NULL CHECK(v4 BETWEEN '1 mon' AND '1 y')
     -- df: size=11 offset=1 unit='mon'
, v5 INTERVAL NOT NULL -- df: size=100 offset=0 unit='y'
, v6 INTERVAL NOT NULL -- df: size=1000000 offset=0 unit='s'
  -- text
, s0 CHAR(12) UNIQUE NOT NULL
, s1 VARCHAR(15) UNIQUE NOT NULL
, s2 TEXT NOT NULL -- df: length=23 lenvar=1 size=20 seed=s2
, s3 TEXT NOT NULL CHECK(s3 LIKE 'stuff%') -- df: prefix='stuff'
, s4 TEXT NOT NULL CHECK(s4 ~ '^[a-f]{9,11}$')
    -- df: chars='abcdef' size=20 length=10 lenvar=1
-- df skewed: gen=scale rate=0.7 seed='Calvin and Hobbes are good friends!'
, s5 TEXT NOT NULL CHECK(s5 ~ '^[ab]{20}$')
    --df: chars='ab' size=50 length=20 lenvar=0 cgen=skewed
, s6 TEXT NOT NULL -- df: word=:calvin,hobbes,susie
, s7 TEXT NOT NULL -- df: word=:one,two,three,four,five,six,seven size=3 mangle
, s8 TEXT NOT NULL CHECK(s8 ~ '^((un|deux) ){3}(un|deux)$')
    -- df: text word=:un,deux length=4 lenvar=0
, s9 VARCHAR(10) NOT NULL CHECK(LENGTH(s9) BETWEEN 8 AND 10)
  -- df: length=9 lenvar=1
, sa VARCHAR(8) NOT NULL CHECK(LENGTH(sa) BETWEEN 5 AND 7) -- df: lenvar=1
  -- user defined enum
, e0 df.color NOT NULL
  -- blob
, l0 BYTEA NOT NULL
, l1 BYTEA NOT NULL CHECK(LENGTH(l1) = 3) -- df: length=3 lenvar=0
, l2 BYTEA NOT NULL CHECK(LENGTH(l2) BETWEEN 0 AND 6) -- df: length=3 lenvar=3
);

CREATE TABLE df.ForeignKeys( -- df: mult=2.0
  id SERIAL PRIMARY KEY
, fk1 INTEGER NOT NULL REFERENCES df.stuff
, fk2 INTEGER REFERENCES df.Stuff -- df: null=0.5
, fk3 INTEGER NOT NULL REFERENCES df.Stuff -- df: gen=serial
, fk4 INTEGER NOT NULL REFERENCES df.Stuff -- df: gen=serial mangle
, fk5 INTEGER NOT NULL REFERENCES df.Stuff -- df: gen=serand nomangle
, fk6 INTEGER NOT NULL REFERENCES df.Stuff -- df: gen=serand mangle
, fk7 INTEGER NOT NULL REFERENCES df.Stuff(id) -- df: gen=serand mangle
, fk8 INTEGER NOT NULL REFERENCES df.stuff(i3) -- df: gen=serand mangle
, fk9 INTEGER NOT NULL REFERENCES df.stuff(i3) -- df: gen=uniform
, fka INTEGER NOT NULL REFERENCES df.stuff(i3) -- df: gen=scale rate=0.2
, fkb CHAR(12) NOT NULL REFERENCES df.stuff(s0)
);

CREATE TABLE df.NotFilled( -- df: nogen
  id SERIAL PRIMARY KEY CHECK(id=1)
);
INSERT INTO df.NotFilled(id) VALUES(1);

CREATE TABLE df.Ten( -- df: size=10 null=1.0
  id SERIAL PRIMARY KEY CHECK(id BETWEEN 18 AND 27) -- df: offset=18
, nogen INTEGER DEFAULT 123 -- df: nogen
, n TEXT
  -- forced generators
, x0 TEXT NOT NULL CHECK(x0 ~ '^[0-9]+$') -- df: type=int size=1000
, x1 TEXT NOT NULL CHECK(x1 = 'TRUE' OR x1 = 'FALSE') -- df: type=bool
, x2 TEXT NOT NULL CHECK(x2::DOUBLE PRECISION >=0 AND
                         x2::DOUBLE PRECISION <= 100.0)
                         -- df: type=float alpha=0.0 beta=100.0
, x3 TEXT NOT NULL CHECK(x3 ~ '^\d{4}-\d\d-\d\d$') -- df: type=date
, x4 TEXT NOT NULL CHECK(x4 ~ '^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d$')
                        -- df: type=timestamp
, x5 TEXT NOT NULL CHECK(x5 LIKE 'boo%') -- df: type=string prefix=boo
, x6 TEXT NOT NULL CHECK(x6 ~ '^\d+\s\w+$') -- df: type=interval unit='day'
  -- more forced generators
, y0 INTEGER NOT NULL CHECK(y0 BETWEEN 2 AND 29)
     -- df: type=word word=:2,3,5,7,11,13,17,19,23,29
, y1 BOOLEAN NOT NULL -- df: type=word word=:TRUE,FALSE
, y2 DOUBLE PRECISION NOT NULL CHECK(y2=0.0 OR y2=1.0)
     -- df: type=word word=:0.0,1.0
, y3 FLOAT NOT NULL CHECK(y3=0.0 OR y3=1.0)
     -- df: type=word word=:0.0,1.0
, y4 DATE NOT NULL CHECK(y4 = DATE '2013-06-23' OR y4 = DATE '2038-01-19')
     -- df: type=word word=:2013-06-23,2038-01-19
, y5 TIMESTAMP NOT NULL CHECK(y5 = TIMESTAMP '2013-06-23 19:54:55')
     -- df: type=word word=':2013-06-23 19:54:55'
, y6 INTEGER NOT NULL CHECK(y6::TEXT ~ '^[4-8]{1,9}$')
     -- df: type=chars chars='45678' length=5 lenvar=4 size=1000000
, y7 INTERVAL NOT NULL
     -- df: type=word word=:1y,1mon,1day,1h,1m,1s
);

CREATE TABLE df.Skip( -- df: skip=0.9 size=1000
  id SERIAL PRIMARY KEY
);

"""

VALIDATE_CHECK = """
-- useful for additional checks
CREATE OR REPLACE FUNCTION df.assert(what TEXT, ok BOOLEAN) RETURNS BOOLEAN
IMMUTABLE CALLED ON NULL INPUT AS $$
BEGIN
  IF ok IS NULL OR NOT ok THEN
    RAISE EXCEPTION 'assert failed: %', what;
  END IF;
  RETURN ok;
END
$$ LANGUAGE plpgsql;

-- one if true else 0
CREATE FUNCTION df.oitez(ok BOOLEAN) RETURNS INTEGER
IMMUTABLE STRICT AS $$
  SELECT CASE WHEN ok THEN 1 ELSE 0 END;
$$ LANGUAGE SQL;

-- check value to a precision
CREATE FUNCTION df.value(d DOUBLE PRECISION,
                         val DOUBLE PRECISION, epsilon DOUBLE PRECISION)
RETURNS BOOLEAN
IMMUTABLE STRICT AS $$
  SELECT d BETWEEN val-epsilon AND val+epsilon;
$$ LANGUAGE SQL;

\\echo '# generator checks'
SELECT
  -- check ints
  df.assert('skewed power',
      df.value(AVG(df.oitez(i4=1)), 0.7, 0.1)) AS "i4"
, df.assert('skewed scale',
      df.value(AVG(df.oitez(i5=1)), 0.7, 0.1)) AS "i5"

  -- check bools
, df.assert('b0 rate',
      df.value(AVG(df.oitez(b0)), 0.5, 0.1) AND
      df.value(AVG(df.oitez(b0)), 0.5, 0.1)) AS "b0"
, df.assert('b1 rates',
      df.value(AVG(df.oitez(b1 IS NULL)), 0.5, 0.1) AND
      df.value(AVG(df.oitez(b1)), 0.5, 0.1)) AS "b1"
, df.assert('b2 0.7 rate',
      df.value(AVG(df.oitez(b2)), 0.7, 0.1)) AS "b2"

  -- check floats
, df.assert('uniform', -- 0.5 +- 0.29
      df.value(AVG(f0), 0.5, 0.05) AND df.value(STDDEV(f0), 0.289, 0.05))
      AS "f0"
, df.assert('gaussian',
      df.value(AVG(f1), 5.0, 0.5) AND df.value(STDDEV(f1), 2.0, 0.5))
      AS "f1"
, df.assert('uniform 2',
      df.value(AVG(f2), 0.0, 1.0) AND df.value(STDDEV(f2), 5.77, 0.5))
      AS "f2"
, df.assert('exponential', df.value(AVG(f4), 1.0/0.1, 1.0)) AS "f4"
, df.assert('normal',
      df.value(AVG(f7), 20.0, 0.1) AND df.value(STDDEV(f7), 0.5, 0.1)) AS "f7"

  -- check dates
, df.assert('d4 days', COUNT(DISTINCT d4)=2) AS "d4"
, df.assert('d5 days', COUNT(DISTINCT d5)=2) AS "d5"

  -- check timestamps
, df.assert('t0 stamps', COUNT(DISTINCT t0)=2) AS "t0"
, df.assert('t1 stamps', COUNT(DISTINCT t1)=2) AS "t1"
, df.assert('t2 stamps', COUNT(DISTINCT t2)=30) AS "t2"

  -- check text
, df.assert('s2 text', COUNT(DISTINCT s2)=20 AND
                       MAX(LENGTH(s2))-MIN(LENGTH(s2)) = 2) AS "s2"
, df.assert('s4 text', COUNT(DISTINCT s4)=20) AS "s4"
, df.assert('s5 text', COUNT(DISTINCT s5)=50 AND
      df.value(AVG(LENGTH(REPLACE(s5,'b',''))), 20*0.7, 1.0)) AS "s5"
, df.assert('s6 text', COUNT(DISTINCT s6)=3) AS "s6"
, df.assert('s7 text', COUNT(DISTINCT s7)=3) AS "s7"
, df.assert('s8 text', COUNT(DISTINCT s8)=16) AS "s8"
FROM df.stuff;

\\echo '# foreign key checks'
SELECT
  df.assert('fk2', df.value(AVG(df.oitez(fk2 IS NULL)), 0.5, 0.1)) AS "fk2"
, df.assert('fka', df.value(AVG(df.oitez(fka = 1000000)), 0.2, 0.05)) AS "fka"
FROM df.ForeignKeys;

\\echo '# miscellaneous checks'
SELECT
  df.assert('ten', COUNT(*) = 10) AS "ten"
, df.assert('123', SUM(df.oitez(nogen=123)) = 10) AS "123"
, df.assert('null', SUM(df.oitez(n IS NULL)) = 10) AS "null"
FROM df.Ten;

\\echo '# skip check'
SELECT
  df.assert('skip', COUNT(*) BETWEEN 50 AND 150) AS "skip"
FROM df.Skip;

DROP SCHEMA df CASCADE;
"""

COMICS = """
  -- Comics didactic example.

  \\set ON_ERROR_STOP

  -- Set default scale to 10 tuples for one unit (1.0).
  -- This can be overwritten with the size option.
  -- df: size=10

  -- This relation is not re-generated.
  -- However the size will be used when generating foreign keys to this table.
  CREATE TABLE Language( --df: nogen size=2
    lid SERIAL PRIMARY KEY,
    lang TEXT UNIQUE NOT NULL
  );

  INSERT INTO Language(lid, lang) VALUES
    (1, 'French'),
    (2, 'English')
  ;

  -- Define a char generator for names:
  -- df chfr: gen=scale rate=0.17
  -- df name: chars='esaitnrulodcpmvqfbghjxyzwk' cgen=chfr length=8 lenvar=3

  CREATE TABLE Author( --df: mult=1.0
    aid SERIAL PRIMARY KEY,
    -- There are 400 firstnames do draw from, most frequent 2%.
    -- In the 19th century John & Mary were given to >5% of the US population,
    -- and rates reach 20% in England in 1800. Currently firstnames are much
    -- more diverse, most frequent at about 1%.
    firstname TEXT NOT NULL, -- df: use=name size=200 gen=scale rate=0.02
    -- There are 10000 lastnames to draw from, most frequent 8/1000 (eg Smith)
    lastname TEXT NOT NULL,  -- df: use=name size=10000 gen=scale rate=0.008
    -- Choose dates in the 20th century
    birth DATE NOT NULL,     -- df: start=1901-01-01 end=2000-12-31
    -- We assume that no two authors of the same name are born on the same day
    UNIQUE(firstname, lastname, birth)
  );

  -- On average, about 10 authors per publisher (1.0/0.1)
  CREATE TABLE Publisher( -- df: mult=0.1
    pid SERIAL PRIMARY KEY,
    pname TEXT UNIQUE NOT NULL -- df: prefix=pub length=12 lenvar=4
  );

  -- On average, about 15.1 books per author (15.1/1.0)
  CREATE TABLE Comics( -- df: mult=15.1
    cid SERIAL PRIMARY KEY,
    title TEXT NOT NULL,     -- df: use=name length=20 lenvar=12
    published DATE NOT NULL, -- df: start=1945-01-01 end=2013-06-25
    -- The biased scale generator is set for 95% 1 and 5% for others.
    -- There are 2 language values because of size=2 on table Language.
    lid INTEGER NOT NULL REFERENCES Language, -- df: gen=scale rate=0.95
    pid INTEGER NOT NULL REFERENCES Publisher,
    -- A publisher does not publish a title twice in a day
    UNIQUE(title, published, pid)
  );

  -- Most books are stored once in the inventory.
  -- About 1% of the books are not in the inventory (skip).
  -- Some may be there twice or more (15.2 > 15.1 on Comics).
  CREATE TABLE Inventory( -- df: mult=15.2 skip=0.01
    iid SERIAL PRIMARY KEY, -- df: nomangle
    cid INTEGER NOT NULL REFERENCES Comics, -- df: gen=serand
    shelf INTEGER NOT NULL -- df: size=20
  );

  -- on average, about 2.2 authors per comics (33.2/15.1)
  CREATE TABLE Written( -- df: mult=33.2
    -- serand => at least one per author and one per comics, then random
    cid INTEGER NOT NULL REFERENCES Comics, -- df: gen=serand mangle
    aid INTEGER NOT NULL REFERENCES Author, -- df: gen=serand mangle
    PRIMARY KEY(cid, aid)
  );

"""

# re helpers: alas this is not a parser.

# identifier
re_ident=r'"[^"]+"|`[^`]+`|[a-z0-9_]+'
# possibly schema-qualified
# ??? this won't work with quoted identifiers?
re_ident2=r'({0})\.({0})|{0}'.format(re_ident)

re_cmd=r'CREATE|ALTER|DROP|SELECT|INSERT|UPDATE|DELETE|SET|GRANT|REVOKE|SHOW'
re_ser=r'(SMALL|BIG)?SERIAL|SERIAL[248]'
re_blo=r'BYTEA|BLOB'
re_int=r'{0}|(TINY|SMALL|MEDIUM)INT|INT[248]|INTEGER'.format(re_ser)
re_flt=r'REAL|FLOAT|DOUBLE\s+PRECISION|NUMERIC|DECIMAL'
re_txt=r'TEXT|CHAR\(\d+\)|VARCHAR\(\d+\)'
re_tstz=r'TIMESTAMP(\s+WITH\s+TIME\s+ZONE)?'
re_intv=r'INTERVAL'
re_tim=r'DATE|{0}|{1}'.format(re_tstz, re_intv)
re_boo=r'BOOL(EAN)?'

re_type='|'.join([re_int, re_flt, re_txt, re_tim, re_boo, re_blo])

# SQL syntax
new_object = re.compile(r"^\s*({0})\s".format(re_cmd), re.I)
create_table = \
    re.compile(r'^\s*CREATE\s+TABLE\s*({0})\s*\('.format(re_ident2), re.I)
create_enum = \
    re.compile(r'^\s*CREATE\s+TYPE\s+({0})\s+AS\s+ENUM'.format(re_ident2))
column = re.compile(r'^\s*,?\s*({0})\s+({1})'.format(re_ident, re_type), re.I)
reference = \
  re.compile(r'.*\sREFERENCES\s+({0})\s*(\(({1})\))?'. \
             format(re_ident2, re_ident), re.I)
primary_key = re.compile('.*\sPRIMARY\s+KEY', re.I)
unique = re.compile(r'.*\sUNIQUE', re.I)
not_null = re.compile(r'.*\sNOT\s+NULL', re.I)
unicity = re.compile(r'^\s*(UNIQUE|PRIMARY\s+KEY)\s*\(([^\)]+)\)', re.I)

# detect datafiller directives
df_mac = re.compile(r'.*--\s*df\s+(\w+)\s*:\s*(.*)')
df_dir = re.compile(r'.*--\s*df\s*:\s*(.*)')
df_txt = re.compile(r'(\w+)=\'([^\']*)\'\s+(.*)')
df_flt = re.compile(r'(\w+)=(-?\d+\.\d*)\s+(.*)')
df_int = re.compile(r'(\w+)=(-?\d+)\s+(.*)')
df_str = re.compile(r'(\w+)=(\S*)\s+(.*)')
df_bol = re.compile(r'(\w+)\s+(.*)')

# remove SQL comments & \xxx commands
comments = re.compile(r'(.*?)\s*--.*')
backslash = re.compile(r'\s*\\')

import random

#
# DATA GENERATORS, with some inheritance
#
class Generator:
    def __init__(self, att, params=None):
        self.att = att
        if params == None and att != None:
            params = att.params
        self.params = params
        self.nullp = 0.0
        if att != None and att.isNullable():
            self.nullp = params['null'] if 'null' in params else \
                self.att.table.params['null'] \
                    if 'null' in self.att.table.params else \
                opts.null
        assert self.nullp >= 0.0 and self.nullp <= 1.0, "nullp in [0,1]"
        self.gens, self.size = 0, None
        if 'seed' in self.params:
	    # attribute-level seed
            self.random = random.Random()
            self.random.seed(self.params['seed'])
        else:
            # by default, rely on shared random generator
            self.random = random
    def __str__(self):
        return "{0} size={1} gens={2}".format(type(self), self.size, self.gens)
    def genData(self): # actual data generation
        raise Exception("not implemented in abstract class")
    def getData(self): # get either NULL or a generated data
        # possibly generate a NULL
        if self.nullp != 0.0 and self.random.random() < self.nullp:
            return db.null()
        else:
            return self.genData()

class BoolGenerator(Generator):
    def __init__(self, att, params=None):
        Generator.__init__(self, att, params)
        self.rate = self.params.get('rate', 0.5)
        assert self.rate >= 0.0 and self.rate <= 1.0, "rate in [0,1]"
    def genData(self):
        return False if self.rate == 0.0 else \
               True  if self.rate == 1.0 else \
               self.random.random() < self.rate

class FloatGenerator(Generator):
    def __init__(self, att, params=None):
        Generator.__init__(self, att, params)
        self.type = self.params.get('gen', 'uniform')
        self.alpha = self.params.get('alpha', 0.0)
        self.beta = self.params.get('beta', 1.0)
        # genData() is overwritten depending on type
        r, a, b, t = self.random, self.alpha, self.beta, self.type
        self.genData = \
            (lambda: r.gauss(a, b))           if t == 'gauss'    else \
            (lambda: r.betavariate(a, b))     if t == 'beta'     else \
            (lambda: r.expovariate(a))        if t == 'exp'      else \
            (lambda: r.gammavariate(a, b))    if t == 'gamma'    else \
            (lambda: r.lognormvariate(a, b))  if t == 'log'      else \
            (lambda: r.normalvariate(a, b))   if t == 'norm'     else \
            (lambda: r.paretovariate(a))      if t == 'pareto'   else \
            (lambda: r.uniform(a, b))         if t == 'uniform'  else \
            (lambda: r.vonmisesvariate(a, b)) if t == 'vonmises' else \
            (lambda: r.weibullvariate(a, b))  if t == 'weibull'  else \
            None
        if self.genData == None:
            raise Exception("unexpected float generator type {0}". \
                            format(self.type))

from fractions import gcd
import math

class IntGenerator(Generator):
    # handy primes for step mangling
    primes = [ 107, 127, 149, 163, 197, 229, 269, 317, 389, 449, 547, 631, 733,
               839, 977, 1063, 1181, 1259, 1511, 1789, 2003, 2251, 2503, 2749,
               3001, 4001, 5003, 6007, 7001, 8009, 9973, 19937, 39847, 79693 ]
    def __init__(self, att, params=None):
        Generator.__init__(self, att, params)
        # set generator type depending on attribute
        self.type = self.params['gen'] if 'gen' in self.params else \
                    'serial' if att != None and att.isUnique() else \
                    'uniform'
        # set offset from different sources
        if 'offset' in self.params:
            self.offset = self.params['offset']
        elif att != None and att.isPK and opts.offset:
            self.offset = opts.offset
        elif att != None and att.FK:
            fk = att.FK.getPK()
            self.offset = \
                fk.params.get('offset', opts.offset if opts.offset else 1)
        else:
            self.offset = 1
        # set size, step & shift
        if att != None:
            self.setSize(att.size)
        else:
            self.step = 1
        self.shift = 0
    def setSize(self, size):
        self.size = size
        # nothing to generate...
        if size==0:
            return
        # whether to mangle shift & step
        mangle = opts.mangle or 'mangle' in self.params \
                 if not 'nomangle' in self.params else False
        # set step
        self.step = self.params.get('step')
        if not self.step:
            self.step = IntGenerator.primes[self.random.randrange(0, \
                                len(IntGenerator.primes))] if mangle else 1
        assert self.step != 0, "step must not be zero"
        if gcd(size, self.step) != 1:
            sys.stderr.write("step {0} ignored for size {1}\n".
                             format(self.step, size))
            self.step = 1
        # set shift
        self.shift = self.params['shift'] if 'shift' in self.params else \
                     self.random.randrange(0, size) if mangle else \
                     0
        # get generator parameters, which may depend on size
        if size <= 1:
            return
        if self.type == 'power' or self.type == 'scale':
            if 'alpha' in self.params and 'rate' in self.params:
                raise Exception("cannot specify both alpha & rate")
            if 'alpha' in self.params:
                self.alpha = float(self.params['alpha'])
            elif 'rate' in self.params:
                rate = float(self.params['rate'])
                assert rate > 0.0 and rate < 1.0, "rate in (0,1)"
                if self.type == 'power':
                    self.alpha = - math.log(size) / math.log(rate)
                elif self.type == 'scale':
                    self.alpha = rate * (size - 1.0) / (1.0 - rate)
                else:
                    raise Exception("unexpected generator type {0}". \
                                    format(self.type))
            else:
                self.alpha = 1.0
            assert self.alpha>0, "alpha must be >0, got {:f}".format(self.alpha)
        else:
            self.alpha = None
    def genData(self):
        if self.size == 0:
            raise Exception("cannot draw from empty set")
        # set base in 0..size-1 depending on generator type
        if self.size == 1:
            base = 0
        elif self.type == 'serial' or \
           self.type == 'serand' and self.gens < self.size:
            base = self.gens
        elif self.type == 'uniform' or self.type == 'serand':
            base = int(self.random.randrange(0, self.size))
        elif self.type == 'power':
            base = int(self.size * self.random.random() ** self.alpha)
        elif self.type == 'scale':
            v = self.random.random()
            base = int(self.size * (v / ((1 - self.alpha )*v + self.alpha)))
        else:
            raise Exception("unexpected int generator type {0}". \
                            format(self.type))
        # update counter
        self.gens += 1
        # return possibly mangled result
        return self.offset + (self.shift + self.step * base) % self.size

# This could also be based on FloatGenerator? '4.2 days' is okay for pg.
class IntervalGenerator(IntGenerator):
    def __init__(self, att):
        IntGenerator.__init__(self, att)
        self.unit = self.params.get('unit', 's')
    def genData(self):
        # ??? should not depend on db?
        return db.intervalValue(IntGenerator.genData(self), self.unit)

from datetime import date, timedelta

class DateGenerator(IntGenerator):
    @staticmethod
    def parse(s):
        return datetime.date(datetime.strptime(s, "%Y-%m-%d"))
    def __init__(self, att):
        IntGenerator.__init__(self, att)
        self.offset = 0
        start, end = 'start' in att.params, 'end' in att.params
        ref = att.params['start'] if start else \
              att.params['end'] if end else \
              None
        if ref != None:
            self.ref = DateGenerator.parse(ref)
            self.dir = 2 * ('start' in att.params) - 1
        else:
            self.ref = date.today()
            self.dir = -1
        # precision, defaults to 1 day
        self.prec = att.params.get('prec', 1)
        # adjust size of both start & end are specified
        if start and end:
            dend = DateGenerator.parse(att.params['end'])
            delta = (dend - self.ref) / self.prec
            self.setSize(delta.days+1)
    def genData(self):
        d = self.ref + self.dir * \
            timedelta(days=self.prec * IntGenerator.genData(self))
        return db.dateValue(d)

from datetime import datetime

class TimestampGenerator(IntGenerator):
    @staticmethod
    def parse(s):
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    def __init__(self, att):
        IntGenerator.__init__(self, att)
        self.offset = 0
        self.tz = self.params.get('tz')
        start, end = 'start' in att.params, 'end' in att.params
        ref = att.params['start'] if start else \
              att.params['end'] if end else \
              None
        if ref != None:
            self.ref = TimestampGenerator.parse(ref)
            self.dir = 2 * ('start' in att.params) - 1
        else:
            self.ref = datetime.today()
            self.dir = -1
        # precision, defaults to 60 seconds
        self.prec = att.params.get('prec', 60)
        # set size
        if start and end:
            dend = TimestampGenerator.parse(att.params['end'])
            delta = (dend - self.ref) / self.prec
            self.setSize(delta.total_seconds()+1)
    def genData(self):
        t = self.ref + self.dir * \
            timedelta(seconds=self.prec * IntGenerator.genData(self))
        # TODO: should not depend on db
        return db.timestampValue(t, self.tz)

class StringGenerator(IntGenerator):
    def __init__(self, att):
        IntGenerator.__init__(self, att)
        self.prefix = att.params.get('prefix', att.name)
        # length/var based on type
        clen = re.match(r'.*char\((\d+)\)', att.type)
        self.length, self.lenvar = None, None
        if clen:
            self.length = int(clen.group(1))
            if re.match('varchar', att.type):
                self.lenvar = self.length / 4
                self.length -= self.lenvar
            else: # char(X)
                self.lenvar = 0
        # possibly overwrite from directives
        self.length = att.params.get('length',
                                     self.length if self.length else 12)
        self.lenvar = \
            att.params.get('lenvar',
                self.lenvar if self.lenvar != None else self.length / 4)
        assert self.length >= self.lenvar, "long enough string"
    def lenData(self, length, n):
        sn = '_' + str(n)
        s = self.prefix + sn * int(2 + (length - len(self.prefix)) / len(sn))
        return s[:int(length)]
    def baseData(self, n):
        # data dependent length so as to be deterministic
        s = self.lenData(self.length + self.lenvar, n)
        length = self.length - self.lenvar + hash(s) % (2 * self.lenvar + 1) \
                 if self.lenvar != 0 else self.length
        return s[:int(length)]
    def genData(self):
        return self.baseData(IntGenerator.genData(self))

# two generators are needed, one for the chars & one for the words
# the parameterized inherited generator is used for the words
# BUG: unique is not checked nor structurally inforced
class CharsGenerator(StringGenerator):
    def __init__(self, att, chars):
        StringGenerator.__init__(self, att)
        if att.isUnique():
            raise Exception("chars generator does not support UNIQUE")
        self.chars = chars
        macro = df_macro[att.params['cgen']] \
                if 'cgen' in att.params else { 'gen':'uniform' }
        self.cgen = IntGenerator(att=None, params=macro)
        self.cgen.setSize(len(chars)) # number of chars
        self.cgen.offset = 0
        # overwrite random
        self.cgen.random = random.Random()
    def lenData(self, length, n):
        # be deterministic in n and depend on seed option
        self.cgen.random.seed(opts.seed + str(n) if opts.seed else n)
        s = ''.join([self.chars[self.cgen.getData()] for i in range(length)])
        return s

class WordGenerator(StringGenerator):
    def __init__(self, att, spec, words=None):
        if words:
            self.words = words
        elif spec[0] == ':':
            self.words = spec[1:].split(',')
        else:
            # load word list from file
            f = open(spec)
            assert f, "file {0} is opened".format(spec)
            self.words = [l.rstrip() for l in f]
            f.close()
        StringGenerator.__init__(self, att)
        # TODO: should check that UNIQUE is ok
        # overwrite default size from IntGenerator
        self.setSize(self.params.get('size', len(self.words)))
        # do not access the list out of bounds
    # unused
    lenData = None
    baseData = None
    def setSize(self, size):
        StringGenerator.setSize(self, size)
        if self.size > len(self.words):
            self.size = len(self.words)
        if self.offset + self.size > len(self.words):
            self.offset = 0
    def genData(self):
        return self.words[IntGenerator.genData(self)]

class TextGenerator(WordGenerator):
    def __init__(self, att, spec):
        WordGenerator.__init__(self, att, spec)
        if att.isUnique():
            raise Exception("text generator does not support UNIQUE")
        # number of words to generate
        self.length = att.params.get('length', 15)
        self.lenvar = att.params.get('lenvar', 10)
        assert self.length >= self.lenvar, "long enough text"
    def genData(self):
        length = \
            self.length - self.lenvar + \
                self.random.randrange(0, 2*self.lenvar+1) \
            if self.lenvar != 0 else self.length
        return ' '.join([WordGenerator.genData(self) for i in range(length)])

class BlobGenerator(Generator):
    def __init__(self, att, params=None):
        Generator.__init__(self, att, params)
        self.length = self.params.get('length', 12)
        self.lenvar = self.params.get('lenvar', self.length / 4)
        assert self.length >= self.lenvar, "long enough blob"
    def genData(self):
        len = self.random.randint(self.length-self.lenvar,
                                  self.length+self.lenvar)
        return db.blobValue(self.random.randrange(256) for o in range(len))

# return a dictionnary from line
def getParams(dfline):
    if dfline == '':
        return {}
    params = {}
    dfline += ' '
    while len(dfline)>0:
        # python does not like combining assign & test, so use continue
        d = df_txt.match(dfline)
        if d:
            params[d.group(1)] = str(d.group(2))
            dfline = d.group(3)
            continue
        d = df_flt.match(dfline)
        if d:
            params[d.group(1)] = float(d.group(2))
            dfline = d.group(3)
            continue
        d = df_int.match(dfline)
        if d:
            params[d.group(1)] = int(d.group(2))
            dfline = d.group(3)
            continue
        d = df_str.match(dfline)
        if d:
            # handle use of a macro directly
            if d.group(1) == 'use':
                assert d.group(2) in df_macro, \
                    "macro {0} is defined".format(d.group(2))
                params.update(df_macro[d.group(2)])
            else:
                params[d.group(1)] = d.group(2)
            dfline = d.group(3)
            continue
        d = df_bol.match(dfline)
        if d:
            params[d.group(1)] = True
            dfline = d.group(2)
            continue
        raise Exception("cannot parse: '{0}'".format(dfline))
    return params

#
# Relation model
#
class Model:
    # global parameters
    params = { 'size':int, 'offset':int, 'mangle':bool, 'null':float,
               'seed':str }
    def __init__(self, name):
        if name[0] == '"' or name[0] == '`':
            self.name = name[1:-1]
            self.quoted = True
        else:
            self.name = name.lower()
            self.quoted = False
        self.size = None
        self.params = {}
    def setParams(self, dfline):
        self.params.update(getParams(dfline))
        self.checkParams()
    def checkParams(self):
        params = self.__class__.params
        for k,v in self.params.items():
            assert k in params, "unexpected parameter {0}".format(k)
            if params[k]==float and not (type(v) is float or type(v) is int):
                raise Exception("unexpected type for float parameter {0}". \
                                format(k))
            elif params[k]==int and not type(v) is int:
                raise Exception("unexpected type for int parameter {0}". \
                                format(k))
            # else everythin is fine
    def getName(self):
        return db.quoteIdent(self.name) if self.quoted else self.name

class Attribute(Model):
    # all attribute parameters and their types
    params = {
        # common
        'type':str, 'nogen':bool,
        'size':int, 'mult':float, 'null':float, 'seed':str,
        # int (float)
        'gen':str, 'alpha':float, 'rate':float, 'beta':float,
        'offset':int, 'step':int, 'shift':int,
        'mangle':bool, 'nomangle':bool,
        # bool
        'rate':float,
        # string
        'prefix':str, 'length':int, 'lenvar':int,
        # word & text
        'word':str, 'text':bool,
        # text
        'chars':str, 'cgen':str,
        # date, timestamp & interval
        'start':str, 'end':str, 'prec':int, 'tz':str, 'unit':str
    }
    def __init__(self, name, number, type):
        Model.__init__(self, name)
        self.params['mult'] = 1.0
        self.number = number
        self.type = type.lower()
        self.FK = None
        self.isPK = False
        self.unique = False
        self.not_null = False
        self.gen = None
    def __str__(self):
        return "Attr {0} {1} {2} PK={3} U={4} NN={5} FK=[{6}]". \
            format(self.number, self.name, self.type, \
                   self.isPK, self.unique, self.not_null, self.FK)
    __repr__ = __str__
    def getData(self):
        if self.gen:
            return self.gen.getData()
        raise Exception("no generator set for attribute {0}".format(self.name))
    def checkParams(self):
        Model.checkParams(self)
        if 'cgen' in self.params and not 'chars' in self.params:
            raise Exception('cgen directive requires chars directive')
        if 'word' in self.params and 'chars' in self.params:
            raise Exception('must choose one of word & chars')
        if 'mangle' in self.params and 'nomangle' in self.params:
            raise Exception('mangle and nomangle are exclusive')
        if 'alpha' in self.params and 'rate' in self.params:
            raise Exception('alpha and rate are exclusive')
        # other consistency checks would be possible
    def isUnique(self):
        return self.isPK or self.unique
    def isNullable(self):
        return not self.not_null and not self.isPK
    def isSerial(self):
        return db.serialType(self.type)

class Table(Model):
    params = { 'mult':float, 'size':int, 'nogen':bool,
               'skip':float, 'null':float }
    def __init__(self, name):
        Model.__init__(self, name)
        self.params['mult'] = 1.0
        # attributes
        self.atts = {}
        self.att_list = [] # list of attributes in occurrence order
        self.unique = []
        self.ustuff = {} # uniques are registered in this dictionnary
        self.constraints = []
    def __str__(self):
        return "Table {0} ({1:d})".format(self.name, self.size)
    def __repr__(self):
        s = str(self) + "\n"
        for att in self.att_list:
            s += "  " + repr(att) + "\n"
        for u in self.unique:
            s += "  UNIQUE: " + str(u) + "\n"
        s += "  CONSTRAINTS: " + str(self.constraints) + "\n"
        return s
    def addAttribute(self, att):
        self.att_list.append(att)
        self.atts[att.name] = att
        # point back
        att.table = self
        # pg-specific generated constraint names
        if att.isPK:
            self.constraints.append(self.name + '_pkey')
        elif att.isUnique():
            self.constraints.append(self.name + '_' + att.name + '_key')
        elif att.FK:
            self.constraints.append(self.name + '_' + att.name + '_fkey')
    def addUnique(self, atts, type=None):
        self.unique.append([self.atts[a.lower()].number for a in atts])
        self.constraints.append(self.name + '_' + \
                                '_'.join([a.lower() for a in atts]) +
                                ('_key' if type.lower()=='unique' else '_pkey'))
    def getAttribute(self, name):
        return self.atts[name.lower()]
    def getPK(self):
        for a in self.att_list:
            if a.isPK:
                return a
        raise Exception("no PK found in table {0}".format(self.name))
    def getData(self):
        tries = opts.tries
        while tries:
            tries -= 1
            nu, collision = 0, False
            sul= []
            l = [a.getData() for a in filter(lambda x: x.gen, self.att_list)]
            for u in self.unique:
                nu += 1
                su = str(nu) + ':' + str([l[i-1] for i in u])
                if su in self.ustuff:
                    collision = True
                    break # non unique tuple
                else:
                    sul.append(su)
            if collision:
                continue # restart while
            else:
                for su in sul:
                    self.ustuff[su] = 1
                return l
        raise Exception("cannot build tuple for table {0}".format(self.name))

#
# Databases
#

class Database:
    def echo(self, s):
        raise Exception('not implemented in abstract class')
    # transactions
    def begin(self):
        raise Exception('not implemented in abstract class')
    def commit(self):
        raise Exception('not implemented in abstract class')
    # operations
    def insertBegin(self, table):
        raise Exception('not implemented in abstract class')
    def insertValue(self, table, value, isLast):
        raise Exception('not implemented in abstract class')
    def insertEnd(self):
        raise Exception('not implemented in abstract class')
    def setSequence(self, att, number):
        raise Exception('not implemented in abstract class')
    def dropTable(self, table):
        return "DROP TABLE {0};".format(self.quote_ident(table.name))
    def truncateTable(self, table):
        return "DELETE FROM {0};".format(self.quote_ident(table.name))
    # types
    def serialType(self, type):
        return False
    def intType(self, type):
        t = type.lower()
        return t == 'smallint' or t == 'int' or t == 'integer' or t == 'bigint'
    def textType(self, type):
        t = type.lower()
        return t == 'text' or re.match(r'(var)?char\(', t)
    def boolType(self, type):
        t = type.lower()
        return t == 'bool' or t == 'boolean'
    def dateType(self, type):
        return type.lower() == 'date'
    def intervalType(self, type):
        return type.lower() == 'interval'
    def timestampType(self, type):
        return re.match(re_tstz + '$', type, re.I)
    def floatType(self, type):
        return re.match(re_flt + '$', type, re.I)
    def blobType(self, type):
        return re.match(re_blo + '$', type, re.I)
    # quoting
    def quoteIdent(self, ident):
        raise Exception('not implemented in abstract class')
    def quoteLiteral(self, literal):
        return '\'' + literal + '\''
    # values
    def null(self):
        raise Exception('not implemented in abstract class')
    def boolValue(self, b):
        return 'TRUE' if b else 'FALSE'
    def dateValue(self, d):
        return d.strftime('%Y-%m-%d')
    def timestampValue(self, t, tz=None):
        ts = t.strftime('%Y-%m-%d %H:%M:%S')
        if tz:
            ts += ' ' + tz
        return ts
    def intervalValue(self, val, unit):
        return str(val) + ' ' + unit
    def blobValue(self, lo):
        raise Exception('not implemented in abstract class')

class PostgreSQL(Database):
    def echo(self, s):
        return '\\echo ' + s
    def begin(self):
        return 'BEGIN;'
    def commit(self):
        return 'COMMIT;'
    def insertBegin(self, table):
        return "COPY {0} ({1}) FROM STDIN;".format(table.getName(),
             ','.join([a.getName() \
                       for a in filter(lambda x: x.gen, table.att_list)]))
    def insertValue(self, table, value, isLast):
        return '\t'.join([self.boolValue(i) if type(i) is bool else str(i)
                          for i in value])
    def insertEnd(self):
        return '\\.'
    def setSequence(self, tab, att, number):
        name = "{0}_{1}_seq".format(tab.name, att.name)
        if tab.quoted or att.quoted:
            name = db.quoteIdent(name)
        return "ALTER SEQUENCE {0} RESTART WITH {1};".format(name, number)
    def dropTable(self, table):
        return "DROP TABLE IF EXISTS {0};".format(table.getName())
    def truncateTable(self, table):
        return "TRUNCATE TABLE {0} CASCADE;".format(table.getName())
    def quoteIdent(self, ident):
        return '"{0}"'.format(ident)
    def null(self):
        return r'\N'
    def serialType(self, type):
        return re.match('(' + re_ser + ')$', type, re.I)
    def intType(self, type):
        t = type.lower()
        return Database.intType(self, t) or self.serialType(type)
    def blobValue(self, lo):
        return r'\\x' + ''.join(["{:02x}".format(o) for o in lo])

class MySQL(Database):
    def begin(self):
        return 'START TRANSACTION;'
    def commit(self):
        return 'COMMIT;'
    def insertBegin(self, table):
        return "INSERT INTO {0} ({1}) VALUES".format(table.getName(),
                        ','.join([a.getName() for a in table.att_list]))
    def insertValue(self, table, value, isLast):
        qv = []
        for v in value:
            qv.append(self.boolValue(v) if type(v) is bool else
                      self.quoteLiteral(v) if type(v) is str else str(v))
        s = '  (' + ','.join([str(v) for v in qv]) + ')'
        if not isLast:
            s += ','
        return s
    def insertEnd(self):
        return ';'
    def null(self):
        return 'NULL'
    def intType(self, type):
        t = type.lower()
        return Database.intType(self, t) or t == 'tinyint' or t == 'mediumint'

# class SQLite(Database):
# class CSV(Database):

# option management
# --size=1000
# --target=postgresql|mysql
# --help is automatic
import sys
import argparse
opts = argparse.ArgumentParser(version="version {0}".format(version),
                    description='Fill database tables with random data.')
opts.add_argument('-s', '--size', type=int, default=None,
                  help='scale to size')
opts.add_argument('-t', '--target', default='postgresql',
                  help='generate for this engine')
opts.add_argument('-f', '--filter', action='store_true', default=False,
                  help='also include input in output')
opts.add_argument('-T', '--transaction', action='store_true',
                  help='wrap output in a transaction')
opts.add_argument('-S', '--seed', default=None,
                  help='random generator seed')
opts.add_argument('-O', '--offset', type=int, default=None,
                  help='set global offset for integer primary keys')
opts.add_argument('-M', '--mangle', action='store_true', default=False,
                  help='use a random step for integer generation')
opts.add_argument('--truncate', action='store_true', default=False,
                  help='truncate table contents before loading')
opts.add_argument('--drop', action='store_true', default=False,
                  help='drop tables before reloading')
opts.add_argument('-D', '--debug', action='count',
                  help='set debug mode')
opts.add_argument('-m', '--man', action='store_const', const=2,
                  help='show man page')
opts.add_argument('-n', '--null', type=float, default=None,
                  help='probability of generating a NULL value')
opts.add_argument('--pod', type=str, default='pod2usage -verbose 3',
                  help='override pod2usage command')
opts.add_argument('--test', default=None,
                  help='show output for an example')
opts.add_argument('--validate', action='store_true', default=False,
                  help='shortcut for script validation')
opts.add_argument('--tries', type=int, default=10,
                  help='how hard to try to satisfy unique constraints')
opts.add_argument('-V', action='store_true', default=False,
                  help='show short version on stdout')
opts.add_argument('file', nargs='*',
                  help='process files, or stdin if empty')
opts = opts.parse_args()

if opts.V:
    print(VERSION)
    sys.exit(0)

# set database
db = None
if opts.target == 'postgresql':
    db = PostgreSQL()
elif opts.target == 'mysql':
    db = MySQL()
else:
    raise Exception("unexpected target database {0}".format(opts.target))

# shortcut
if opts.validate:
    opts.test = 'validate'
    opts.transaction = True

# int & bool generator tests
test = re.match(r'(\w+):\s*(.*)', opts.test if opts.test else '')
if test:
    ttype, params = test.group(1), getParams(test.group(2))
    if ttype == 'bool':
        gen = BoolGenerator(None, params)
        val, n = [ 0, 0 ], 10000
        for i in range(n):
            val[gen.genData()] += 1
        print("True: {:5.2f} %".format(100.0*val[1]/n))
    elif ttype == 'int':
        gen = IntGenerator(None, params)
        gen.offset = 0
        gen.setSize(params.get('size', opts.size if opts.size else 10))
        val, n = [ 0 ] * gen.size, 1000 * gen.size
        for i in range(n):
            val[gen.genData()] += 1
        for i in range(gen.size):
            print("{:4d}  {:5.2f} %".format(i, 100.0*val[i]/n))
    elif ttype == 'float':
        gen = FloatGenerator(None, params)
        size = opts.size if opts.size else 10
        print(sorted([gen.genData() for i in range(size)]))
    elif ttype == 'blob':
        gen = BlobGenerator(None, params)
        for i in range(opts.size if opts.size else 10):
            print(gen.genData())
    else:
        raise Exception("unexpected generator test {0}".format(ttype))
    sys.exit(0)

# option consistency
if opts.drop or opts.test:
    opts.filter = True

if opts.filter and opts.truncate:
    raise Exception("option truncate does not make sense with option filter")

if opts.man:
    # hack to have pod from python
    import os, tempfile
    pod = tempfile.NamedTemporaryFile(mode='w')
    pod.write(POD.format(comics=COMICS, pgbench=PGBENCH,
                         script=sys.argv[0], version=version))
    pod.flush()
    os.system(opts.pod + ' ' + pod.name)
    pod.close()
    sys.exit(0)

# reset arguments for fileinput
sys.argv[1:] = opts.file

#
# global variables while parsing
#

# list of tables in occurrence order, for regeneration
tables = []
all_tables = {}

# parser status
current_table = None
current_attribute = None
current_enum = None
dfstuff = None
att_number = 0

# enums
all_enums = {}
re_enums = ''

# schema stores global parameters
schema = Model('df')

# macros: 'name':{}
df_macro = {}
df_macro['cfr'] = getParams('gen=scale rate=0.17')
df_macro['french'] = getParams('chars=\'esaitnrulodcpmvqfbghjxyzwk\' cgen=cfr')
df_macro['cen'] = getParams('gen=scale rate=0.15')
df_macro['english'] = getParams('chars=\'etaonrishdlfcmugypwbvkjxqz\' cgen=cen')

#
# INPUT SCHEMA
#

if opts.test == 'comics':
    lines = StringIO(COMICS).readlines()
elif opts.test == 'pgbench':
    lines = StringIO(PGBENCH).readlines()
elif opts.test == 'validate':
    lines = StringIO(VALIDATE).readlines()
else:
    import fileinput # despite the name this is really a filter...
    lines = [l for l in fileinput.input()]

#
# SCHEMA PARSER
#

# extract a list of sql strings
re_quoted = re.compile(r"[^']*'(([^']|'')*)'(.*)")
def sql_string_list(line):
    sl = []
    quoted = re_quoted.match(line)
    while quoted:
        sl.append(quoted.group(1))
        line = quoted.group(3)
        quoted = re_quoted.match(line)
    return sl

for line in lines:
    if opts.debug:
        sys.stderr.write("line=" + line)
    # skip \commands
    if backslash.match(line):
        continue
    # get datafiller stuff
    d = df_dir.match(line)
    if d:
        dfstuff = d.group(1)
    # get datafiller macro definition
    d = df_mac.match(line)
    if d:
        if d.group(1) in df_macro:
            sys.stderr.write("warning: macro {0} is redefined\n".
                             format(d.group(1)))
        df_macro[d.group(1)] = getParams(d.group(2))
    # cleanup comments
    c = comments.match(line)
    if c:
        line = c.group(1)
    # reset current object
    if new_object.match(line):
        current_table = None
        current_attribute = None
        current_enum = None
        att_number = 0
    #
    # CREATE TYPE ... AS ENUM
    #
    is_ce = create_enum.match(line)
    if is_ce:
        current_enum = is_ce.group(1) # lower()?
        if re_enums:
            re_enums += '|'
        # ??? should escape special characters such as "."
        re_enums += current_enum
        all_enums[current_enum] = sql_string_list(line)
        continue
    # follow up...
    if current_enum:
        all_enums[current_enum].extend(sql_string_list(line))
        continue
    #
    # CREATE TABLE
    #
    is_ct = create_table.match(line)
    if is_ct:
        name = is_ct.group(1)
        current_table = Table(name)
        tables.append(current_table)
        all_tables[name.lower()] = current_table
    elif current_table!=None:
        #
        # COLUMN
        #
        is_enum = False
        c = column.match(line)
        if not c and re_enums:
            r = r'\s*,?\s*({0})\s+({1})'.format(re_ident, re_enums)
            c = re.match(r, line)
            is_enum = bool(c)
        if c:
            att_number += 1
            current_attribute = Attribute(c.group(1), att_number, c.group(2))
            current_attribute.is_enum = is_enum
            if primary_key.match(line):
                current_attribute.isPK = True
            if unique.match(line):
                current_attribute.unique = True
            if not_null.match(line):
                current_attribute.not_null = True
            current_table.addAttribute(current_attribute)
            r = reference.match(line)
            if r:
                target = r.group(1)
                current_attribute.FK = all_tables[target.lower()]
                current_attribute.FKatt = r.group(5) if r.group(4) else None
        else:
            # UNIQUE()
            u = unicity.match(line)
            if u:
                current_table.addUnique(re.split(r'[\s,]+',u.group(2)), \
                                        u.group(1))
    # attribute df stuff to current object: schema, table or attribute
    # this come last if the dfstuff is on the same line as its object
    if dfstuff!=None:
        if current_attribute!=None:
            current_attribute.setParams(dfstuff)
        elif current_table!=None:
            current_table.setParams(dfstuff)
        else:
            schema.setParams(dfstuff)
        dfstuff = None

#
# SET DEFAULT VALUES for some options, possibly from directives
#
if opts.size == None:
    opts.size = schema.params.get('size', 100)

if not opts.offset:
    opts.offset = schema.params.get('offset')

if not opts.mangle:
    opts.mangle = 'mangle' in schema.params

if not opts.null:
    opts.null = schema.params.get('null', 0.01)

if not opts.seed:
    opts.seed = schema.params.get('seed')

# set seed, default uses os random or time
random.seed(opts.seed)

#
# START OUTPUT
#
print("-- data generated by {0} version {1} for {2}".
      format(sys.argv[0], version, opts.target))

if opts.transaction:
    print('')
    print(db.begin())

#
# DROP
#
if opts.drop:
    print('')
    print('-- drop tables')
    for t in reversed(tables):
        print(db.dropTable(t))

#
# SHOW INPUT
#
if opts.filter:
    print('')
    print('-- INPUT FILE BEGIN')
    for line in lines:
        sys.stdout.write(line)
    print('-- INPUT FILE END')

#
# TRUNCATE
#
if opts.truncate:
    print('')
    print('-- truncate tables')
    for t in filter(lambda t: not 'nogen' in t.params, reversed(tables)):
        print(db.truncateTable(t))

#
# SET TABLE AND ATTRIBUTE SIZES
#
# first table sizes
for t in tables:
    # set skip
    t.skip = t.params['skip'] if 'skip' in t.params else 0.0
    assert t.skip >= 0.0 and t.skip <= 1.0
    if t.size==None:
        t.size = t.params['size'] if 'size' in t.params else \
                 int(t.params['mult'] * opts.size)

# *then* set att sizes and possible offset
for t in tables:
    for a in t.att_list:
        if a.FK != None:
            a.size = a.FK.size
            if a.FK.skip:
                raise Exception("reference on table {0} with skipped tuples".
                                format(a.FK.name))
            key = a.FK.atts[a.FKatt] if a.FKatt else a.FK.getPK()
            assert key.isUnique(), \
                "foreign key {0}.{1} target {2} must be unique". \
                format(a.table.name, a.name, key.name)
            # override default prefix
            assert not 'prefix' in a.params, \
                "no prefix on foreign key {0}.{1}".format(a.table.name, a.name)
            a.params['prefix'] = key.params.get('prefix', key.name)
            # transfer all other directives
            for d, v in key.params.items():
                if not d in a.params:
                    a.params[d] = v
        elif 'size' in a.params:
            a.size = a.params['size']
        elif a.size == None:
            a.size = int(t.size * a.params['mult'])

#
# CREATE DATA GENERATORS per attribute
#
for t in tables:
    for a in t.att_list:
        if 'nogen' in a.params:
            a.gen = None
        elif 'type' in a.params:
            t = a.params['type']
            a.gen = IntGenerator(a) if t == 'int' else \
                    BoolGenerator(a) if t == 'bool' else \
                    FloatGenerator(a) if t == 'float' else \
                    DateGenerator(a) if t == 'date' else \
                    TimestampGenerator(a) if t == 'timestamp' else \
                    IntervalGenerator(a) if t == 'interval' else \
                    StringGenerator(a) if t == 'string' else \
                    CharsGenerator(a, a.params['chars']) if t == 'chars' else \
                    WordGenerator(a, a.params['word']) if t == 'word' else \
                    TextGenerator(a, a.params['word']) if t == 'text' else \
                    BlobGenerator(a) if t == 'blob' else \
                    None
            assert a.gen, "generator type {0} found".format(t)
        elif 'text' in a.params:
            assert db.textType(a.type), "text attribute for text"
            assert 'word' in a.params, "text generator requires word"
            a.gen = TextGenerator(a, a.params['word'])
        elif 'word' in a.params:
            assert db.textType(a.type), "text attribute for word"
            a.gen = WordGenerator(a, a.params['word'])
        elif 'chars' in a.params:
            assert db.textType(a.type), "text attribute for chars"
            a.gen = CharsGenerator(a, a.params['chars'])
        # type-based default generators
        elif a.is_enum:
            a.gen = WordGenerator(a, None, all_enums[a.type])
        elif db.intType(a.type):
            a.gen = IntGenerator(a)
        elif db.textType(a.type):
            a.gen = StringGenerator(a)
        elif db.boolType(a.type):
            a.gen = BoolGenerator(a)
        elif db.dateType(a.type):
            a.gen = DateGenerator(a)
        elif db.timestampType(a.type):
            a.gen = TimestampGenerator(a)
        elif db.intervalType(a.type):
            a.gen = IntervalGenerator(a)
        elif db.floatType(a.type):
            a.gen = FloatGenerator(a)
        elif db.blobType(a.type):
            a.gen = BlobGenerator(a)
        else:
            a.gen = None

# print tables
if opts.debug:
    sys.stderr.write(tables)

#
# CALL GENERATORS on each table
#
for t in tables:
    print
    if 'nogen' in t.params or t.size == 0:
        print("-- skip table {0}".format(t.name))
    else:
        size = "{:d}*{:g}".format(t.size, 1.0-t.skip) if t.skip else str(t.size)
        print("-- fill table {0} ({1})".format(t.name, size))
        print(db.echo("# filling table {0} ({1})".format(t.name, size)))
        print(db.insertBegin(t))
        for i in range(t.size):
            # the tuple is generated, but may nevertheless not be inserted
            tup = t.getData()
            if not t.skip or not random.random() < t.skip:
                print(db.insertValue(t, tup, i==t.size-1))
        print(db.insertEnd())

#
# RESTART SEQUENCES
#
print('')
print('-- restart sequences')
for t in filter(lambda t: not 'nogen' in t.params, tables):
    for a in filter(lambda a: a.isSerial(), t.att_list):
        print(db.setSequence(t, a, a.gen.offset + a.gen.size))

#
# DONE
#

if opts.transaction:
    print('')
    print(db.commit())

if opts.target == 'postgresql':
    print('')
    print('-- analyze modified tables')
    for t in filter(lambda t: not 'nogen' in t.params, tables):
        print("ANALYZE {0};".format(t.getName()))

#
# validation
#
if opts.test == 'validate':
    print(VALIDATE_CHECK);
