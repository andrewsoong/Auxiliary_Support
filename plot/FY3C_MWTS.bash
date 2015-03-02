#!/bin/bash

MM_SAT="FY3C" 
MM_IST="MWTS"
MM_CHN=1

MM_FILEROOT="/home/f3cmon/MONITOR/py/bin/"
## For the Original data
declare -i -x VALUERANGE=46
declare -i -x VALUERANGE=36
declare -i -x VALUESPAN=3
declare -i -x VALUESUB=18
export MM_SAT

declare -i -x VALUEADD=10
#declare -i -x NPNTIRAS=56
#declare -i -x NPNTMWTS=15
#declare -i -x NPNTMWHS=98
#declare -i -x NPNTVASS=56
#declare -i -x NADDFY3B=25   ## number of var of geo-inform
#declare -i -x NBITFY3B=4

#for channal one
declare -i -x VALUERANGE_CH1=24
declare -i -x VALUESPAN_CH1=2
declare -i -x VALUESUB_CH1=12
declare -i -x VALUEADD_CH1=0

#for channal two
declare -i -x VALUERANGE_CH2=24
declare -i -x VALUESPAN_CH2=2
declare -i -x VALUESUB_CH2=12
declare -i -x VALUEADD_CH2=0

#for channal three
declare -i -x VALUERANGE_CH3=24
declare -i -x VALUESPAN_CH3=2
declare -i -x VALUESUB_CH3=12
declare -i -x VALUEADD_CH3=0

#for channal four
declare -i -x VALUERANGE_CH4=24
declare -i -x VALUESPAN_CH4=2
declare -i -x VALUESUB_CH4=12
declare -i -x VALUEADD_CH4=0

#for channal five
declare -i -x VALUERANGE_CH5=12
declare -i -x VALUESPAN_CH5=1
declare -i -x VALUESUB_CH5=6
declare -r-x VALUEMUL_CH5=1.5
declare -i -x VALUEADD_CH5=0

#for channal six
declare -i -x VALUERANGE_CH6=12
declare -i -x VALUESPAN_CH6=1
declare -i -x VALUESUB_CH6=6
declare -i -x VALUEADD_CH6=0

#for channal seven
declare -i -x VALUERANGE_CH7=12
declare -i -x VALUESPAN_CH7=1
declare -i -x VALUESUB_CH7=6
declare -i -x VALUEADD_CH7=0

#for channal eight
declare -i -x VALUERANGE_CH8=12
declare -i -x VALUESPAN_CH8=1
declare -i -x VALUESUB_CH8=6
declare -i -x VALUEADD_CH8=0

#for channal nine
declare -i -x VALUERANGE_CH9=12
declare -i -x VALUESPAN_CH9=1
declare -i -x VALUESUB_CH9=6
declare -i -x VALUEADD_CH9=0

#for channal ten
declare -i -x VALUERANGE_CH10=12
declare -i -x VALUESPAN_CH10=1
declare -i -x VALUESUB_CH10=6
declare -i -x VALUEADD_CH10=0

#for channal eleven
declare -i -x VALUERANGE_CH11=12
declare -i -x VALUESPAN_CH11=1
declare -i -x VALUESUB_CH11=6
declare -i -x VALUEADD_CH11=0

#for channal twelve
declare -i -x VALUERANGE_CH12=12
declare -i -x VALUESPAN_CH12=1
declare -i -x VALUESUB_CH12=6
declare -i -x VALUEADD_CH12=0

#for channal thirteen
declare -i -x VALUERANGE_CH13=12
declare -i -x VALUESPAN_CH13=1
declare -i -x VALUESUB_CH13=6
declare -i -x VALUEADD_CH13=0
