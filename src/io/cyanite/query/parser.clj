(ns io.cyanite.query.parser
  "Graphite DSL to AST translation"
  (:require [instaparse.core :as parse]))

(def init
  "Instaparse grammar for the Graphite DSL"
  "<expr>         = path | func
   <op>           = <'('>
   <cp>           = <')'>
   <sep>          = <','>
   <number>       = #'[+-]?[0-9]+'
   <float>        = #'[+-]?[0-9]+(\\.[0-9]+)?'
   <aggregator1>  = <'\"'> ( #'(?i)max' | #'(?i)min' | #'(?i)avg' ) <'\"'>
   <aggregator2>  = <'\\''> ( #'(?i)max' | #'(?i)min' | #'(?i)avg' ) <'\\''>
   <aggregator>   = aggregator1 | aggregator2
   <consol1>      = <'\"'> ( #'(?i)max' | #'(?i)min' ) <'\"'>
   <consol2>      = <'\\''> ( #'(?i)max' | #'(?i)min' ) <'\\''>
   <consolidator> = consol1 | consol2
   <qstr1>        = <'\"'> #'(?i)[a-z0-9 .%+*/_-]*'<'\"'>
   <qstr2>        = <'\\''> #'(?i)[a-z0-9 .%+*/_-]*'<'\\''>
   <qstr>         = qstr1 | qstr2
   cactisi        = <#'(?i)si'>
   cactibin       = <#'(?i)bin'>
   <cactisystem1> = <'\"'> ( cactisi | cactibin) <'\"'>
   <cactisystem2> = <'\\''> ( cactisi | cactibin) <'\\''>
   <cactisystem>  = (cactisystem1 | cactisystem2)
   <bool>         = ( #'(?i)true' | #'(?i)false' )
   seconds        = <#'(?i)s(second(s)?)?'>
   min            = <#'(?i)m(in(ute(s)?)?)?'>
   hours          = <#'(?i)h(our(s)?)?'>
   days           = <#'(?i)d(ay(s)?)?'>
   weeks          = <#'(?i)w((eek)?s)?'>
   months         = <#'(?i)mon(th(s)?)?'>
   years          = <#'(?i)y(ear(s)?)?'>
   <quantifier>   = seconds | min | hours | days | weeks | months | years
   <timerange1>   = <'\\''> number quantifier <'\\''>
   <timerange2>   = <'\"'> number quantifier <'\"'>
   timerange      = timerange1 | timerange2
   absolutepoints = number
   <points>       = absolutepoints | timerange

   absolute       = <#'(?i)absolute'> op expr cp
   aggregateline  = <#'(?i)aggregateline'> op expr sep aggregator cp
   alias          = <#'(?i)alias'> op expr sep qstr cp
   aliasmetric    = <#'(?i)aliasbymetric'> op expr cp
   aliasnode      = <#'(?i)aliasbynode'> op expr sep number (sep number)* cp
   aliassub       = <#'(?i)aliassub'> op expr sep qstr sep qstr cp
   alpha          = <#'(?i)alpha'> op expr sep float cp
   areabetween    = <#'(?i)areabetween'> op arglist cp
   aspercent      = <#'(?i)aspercent'> op expr ( sep (expr | number) )? cp
   avgabove       = <#'(?i)averageabove'> op expr sep float cp
   avgbelow       = <#'(?i)averagebelow'> op expr sep float cp
   avgpercentile  = <#'(?i)averageoutsidepercentile'> op expr sep float cp
   avgseries      = (<#'(?i)averageseries' | #'(?i)avg'>) op arglist cp
   <avgwildkw>    = <#'(?i)averageserieswithwildcards'>
   avgwildcards   = <avgwildkw> op expr sep float (sep float)* cp
   <afns1>        = absolute | aggregateline | alias | aliasmetric | aliasnode
   <afns2>        = aliassub | alpha | areabetween | aspercent | avgabove
   <afns3>        = avgbelow | avgpercentile | avgseries | avgwildcards
   <a>            = afns1 | afns2 | afns3

   cactistyle     = <#'(?i)cactistyle'> op expr (sep cactisystem)? cp
   changed        = <#'(?i)changed'> op expr cp
   color          = <#'(?i)color'> op expr sep qstr cp
   consolidate    = <#'(?i)consolidateby'> op expr sep consolidator cp
   constantline   = <#'(?i)constantline'> op float cp
   countseries    = <#'(?i)countseries'> op arglist cp
   cumulative     = <#'(?i)cumulative'> op expr sep consolidator cp
   currentabove   = <#'(?i)currentabove'> op expr sep number cp
   currentbelow   = <#'(?i)currentbelow'> op expr sep number cp
   <cfns1>        = cactistyle | changed | color | consolidate | constantline
   <cfns2>        = countseries | cumulative | currentabove | currentbelow
   <c>            = cfns1 | cfns2

   dashed         = <#'(?i)dashed'> op arglist (sep float)? cp
   derivative     = <#'(?i)derivative'> op expr cp
   diffseries     = <#'(?i)diffseries'> op arglist cp
   diffconstant   = <#'(?i)diffseries'> op expr sep float cp
   divideseries   = <#'(?i)divideseries'> op expr sep expr cp
   drawinfinite   = <#'(?i)drawasinfinite'> op expr cp
   <dfns1>        = dashed | derivative | diffseries | diffconstant
   <dfns2>        = divideseries | drawinfinite
   <d>            = dfns1 | dfns2

   exclude        = <#'(?i)exclude'> op expr sep qstr cp
   <e>            = exclude

   grep           = <#'(?i)grep'> op expr sep qstr cp
   group          = <#'(?i)group'> op arglist cp
   groupbynode    = <#'(?i)groupbynode'> op expr sep number sep qstr cp
   <g>            = grep | group | groupbynode

   highestavg     = <#'(?i)highestaverage'> op expr sep number cp
   highestcurrent = <#'(?i)highestcurrent'> op expr sep number cp
   highestmax     = <#'(?i)highestmax'> op expr sep number cp
   hitcount       = <#'(?i)hitcount'> op expr sep qstr (sep bool) cp
   hwaberration   = <#'(?i)holtwintersaberration'> op expr sep number cp
   hwconfband     = <#'(?i)holtwintersconfidencebands'> op expr sep number cp
   hwconfarea     = <#'(?i)holtwintersconfidencearea'> op expr sep number cp
   hwforecast     = <#'(?i)holtwintersforecast'> op expr cp
   <hfns1>        = highestavg | highestcurrent | highestmax | hitcount
   <hfns2>        = hwaberration | hwconfband | hwconfarea | hwforecast
   <h>            = hfns1 | hfns2

   identity       = <#'(?i)identity'> op expr cp
   integral       = <#'(?i)integral'> op expr cp
   invert         = <#'(?i)invert'> op expr cp
   isnonnull      = <#'(?i)isnonnull'> op expr cp
   <i>            = identity | integral | invert | isnonnull


   keeplastvalue  = <#'(?i)keeplastvalue'> op expr (sep number)? cp
   <k>            = keeplastvalue

   limit          = <#'(?i)limit'> op expr cp
   linewidth      = <#'(?i)linewidth'> op expr sep number cp
   logarithm      = <#'(?i)logarithm'> op expr (sep number)? cp
   lowestavg      = <#'(?i)lowestaverage'> op expr sep number cp
   lowestcurrent  = <#'(?i)lowestcurrent'> op expr sep number cp
   <l>            = limit | linewidth | logarithm | lowestavg | lowestcurrent

   mapseries      = <#'(?i)mapseries'> op expr sep number cp
   maxseries      = <#'(?i)maxseries'> op arglist cp
   maxabove       = <#'(?i)maximumabove'> op expr sep float cp
   maxbelow       = <#'(?i)maximumbelow'> op expr sep float cp
   minseries      = <#'(?i)minseries'> op arglist cp
   minabove       = <#'(?i)minimumabove'> op expr sep float cp
   minbelow       = <#'(?i)minimumbelow'> op expr sep float cp
   mostdeviant    = <#'(?i)mostdeviant'> op expr sep float cp
   movingavg      = <#'(?i)movingaverage'> op expr sep points cp
   movingmedian   = <#'(?i)movingmedian'> op expr sep points cp
   multiplyseries = <#'(?i)multiplyseries'> op arglist cp
   <mfns1>        = mapseries | maxseries | maxabove | maxbelow | minseries
   <mfns2>        = minabove | minbelow | mostdeviant | movingavg
   <mfns3>        = movingmedian | multiplyseries
   <m>            = mfns1 | mfns2 | mfns3

   npercentile    = <#'(?i)npercentile'> op expr sep float cp
   nonnegderive   = <#'(?i)nonnegativederivative'> op expr (sep float)? cp
   <n>            = npercentile | nonnegderive

   offset         = <#'(?i)offset'> op expr sep float cp
   offsettozero   = <#'(?i)offsettozero'> op expr cp
   <o>            = offset | offsettozero

   persecond      = <#'(?i)persecond'> op expr (sep float)? cp
   pctileseries   = <#'(?i)percentileseries'> op expr sep float (sep bool)? cp
   pow            = <#'(?i)pow'> op expr sep number cp
   <p>            = persecond | pctileseries | pow

   randomwalk     = <#'(?i)randomwalkfunction'> op path (sep number)? cp
   rangeseries    = <#'(?i)rangeofseries'> op arglist cp
   <redkw>        = <#'(?i)reduceseries'>
   reduceseries   = redkw op expr sep qstr sep number (sep qstr)* cp
   removeabovepct = <#'(?i)removeabovepercentile'> op expr sep float cp
   removeaboveval = <#'(?i)removeabovevalue'> op expr sep float cp
   removebelowpct = <#'(?i)removebelowpercentile'> op expr sep float cp
   removebelowval = <#'(?i)removebelowvalue'> op expr sep float cp
   removebtwpct   = <#'(?i)removebetweenpercentile'> op expr sep float cp
   <rfns1>        = randomwalk | rangeseries | reduceseries | removeabovepct
   <rfns2>        = removeaboveval | removebelowpct | removebelowval
   <rfns3>        = removebtwpct
   <r>            = rfns1 | rfns2 | rfns3

   scale          = <#'(?i)scale'> op expr sep float cp
   scalesecs      = <#'(?i)scaletoseconds'> op expr sep float cp
   secondyaxis    = <#'(?i)secondyaxis'> op expr cp
   <sinkw>        = <#'(?i)sin(function)?'>
   sinfn          = sinkw op expr (sep float (sep float)?)? cp
   <smartsumkw>   = <#'(?i)smartsummarize'>
   smartsum       = smartsumkw op expr sep qstr (sep qstr (sep bool)?)? cp
   sortmaxima     = <#'(?i)sortbymaxima'> op expr cp
   sortminima     = <#'(?i)sortbyminima'> op expr cp
   sortname       = <#'(?i)sortbyname'> op expr cp
   sqrt           = <#'(?i)squareroot'> op expr cp
   stacked        = <#'(?i)stacked'> op expr (sep qstr)? cp
   stddevseries   = <#'(?i)stddevseries'> op arglist cp
   stdev          = <#'(?i)stdev'> op expr sep number (sep float)? cp
   substr         = <#'(?i)substr'> op expr (sep number (sep number)?)? cp
   sumseries      = <#'(?i)sum(series)?'> op arglist cp
   sumserieswild  = <#'(?i)sumserieswithwildcards'> op expr (sep number)* cp
   <sumkw>        = <#'(?i)summarize'>
   summarize      = sumkw op expr sep points (sep qstr (sep bool)?)? cp
   <sfns1>        = scale | scalesecs | secondyaxis | sinfn | smartsum
   <sfns2>        = sortmaxima | sortminima | sortname | sqrt | stacked
   <sfns3>        = stddevseries | stdev | substr | sumseries | sumserieswild
   <sfns4>        = summarize
   <s>            = sfns1 | sfns2 | sfns3 | sfns4

   threshold      = <#'(?i)threshold'> op float (sep qstr (sep qstr))? cp
   timefn         = <#'(?i)time(function)?'> op path (sep number)? cp
   timeshift      = <#'(?i)timeshift'> op expr sep points (sep bool)? cp
   <tmstackkw>    = <#'(?i)timestack'>
   timestack      = tmstackkw op expr sep points sep number sep number cp
   transformnull  = <#'(?i)transformnull'> op expr (sep number)? cp
   <t>            = threshold | timefn | timeshift | timestack | transformnull

   useabove       = <#'(?i)useabove'> op expr float sep qstr sep qstr cp
   <u>            = useabove

   weightedavg    = <#'(?i)weightedaverage'> op expr sep expr sep number cp
   <w>            = weightedavg


   <uqpath>       = #'(?i)[a-z0-9.*]+'
   <qpath>        = <'\"'> uqpath <'\"'>
   path           = uqpath | qpath
   <func>         = a|c|d|e|g|h|i|k|l|m|n|o|p|r|s|t|u|w
   <arglist>      = (expr <','>)* (Epsilon | expr)")

(def query->tokens
  "The parser for the grammar, yields a naive AST built of tokens"
  (comp
   first
   (parse/parser init)))
