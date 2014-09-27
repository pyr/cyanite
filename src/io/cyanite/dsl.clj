(ns io.cyanite.dsl
  (:require [instaparse.core :as parse]))

(def init
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
   <boolean>      = ( #'(?i)true' | #'(?i)false' )

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
   avgwildcards   = <#'(?i)averageserieswithwildcards'> op expr sep number (sep number)* cp
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
   <d>            = dashed | derivative | diffseries | diffconstant | divideseries | drawinfinite

   exclude        = <#'(?i)exclude'> op expr sep qstr cp
   <e>            = exclude

   grep           = <#'(?i)grep'> op expr sep qstr cp
   group          = <#'(?i)group'> op arglist cp
   groupbynode    = <#'(?i)groupbynode'> op expr sep number sep qstr cp
   <g>            = grep | group | groupbynode

   highestavg     = <#'(?i)highestaverage'> op expr sep number cp
   highestcurrent = <#'(?i)highestcurrent'> op expr sep number cp
   highestmax     = <#'(?i)highestmax'> op expr sep number cp
   hitcount       = <#'(?i)hitcount'> op expr sep qstr (sep boolean) cp
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
   <mfns1>        = mapseries | maxseries | maxabove | maxbelow | minseries
   <mfns2>        = minabove | minbelow | mostdeviant
   <m>            = mfns1 | mfns2

   <uqpath>       = #'(?i)[a-z0-9.*]+'
   <qpath>        = <'\"'> uqpath <'\"'>
   path           = uqpath | qpath
   <func>         = a|c|d|e|g|h|i|k|l|m
   <arglist>      = (expr <','>)* (Epsilon | expr)")

(def query->ast
  (parse/parser init))

(defn extract-paths
  [ast]
  (->> ast
       (filter (comp (partial = :path) first))
       (map last)
       (set)))
