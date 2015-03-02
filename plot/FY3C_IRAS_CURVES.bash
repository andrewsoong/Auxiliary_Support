#!/bin/bash

MM_SAT="FY3C" 
MM_IST="IRAS"
MM_CHN=20

MM_FILEROOT="/home/f3cmon/MONITOR/py/bin/"

#for channal one
declare -i -x YMIN_NUMX_CH1=0
declare -i -x YMAX_NUMX_CH1=3
declare -i -x YMIN_BIAS_CH1=-4
declare -i -x YMAX_BIAS_CH1=10
declare -i -x YMIN_STDD_CH1=0
declare -i -x YMAX_STDD_CH1=10

#for channal two
declare -i -x YMIN_NUMX_CH2=0
declare -i -x YMAX_NUMX_CH2=3
declare -i -x YMIN_BIAS_CH2=-4
declare -i -x YMAX_BIAS_CH2=10
declare -i -x YMIN_STDD_CH2=0
declare -i -x YMAX_STDD_CH2=10

#for channal three
declare -i -x YMIN_NUMX_CH3=0
declare -i -x YMAX_NUMX_CH3=3
declare -i -x YMIN_BIAS_CH3=-4
declare -i -x YMAX_BIAS_CH3=10
declare -i -x YMIN_STDD_CH3=0
declare -i -x YMAX_STDD_CH3=10

#for channal four
declare -i -x YMIN_NUMX_CH4=0
declare -i -x YMAX_NUMX_CH4=3
declare -i -x YMIN_BIAS_CH4=-4
declare -i -x YMAX_BIAS_CH4=10
declare -i -x YMIN_STDD_CH4=0
declare -i -x YMAX_STDD_CH4=10

#for channal five
declare -i -x YMIN_NUMX_CH5=0
declare -i -x YMAX_NUMX_CH5=3
declare -i -x YMIN_BIAS_CH5=-4
declare -i -x YMAX_BIAS_CH5=10
declare -i -x YMIN_STDD_CH5=0
declare -i -x YMAX_STDD_CH5=10

#for channal six
declare -i -x YMIN_NUMX_CH6=0
declare -i -x YMAX_NUMX_CH6=3
declare -i -x YMIN_BIAS_CH6=-4
declare -i -x YMAX_BIAS_CH6=10
declare -i -x YMIN_STDD_CH6=0
declare -i -x YMAX_STDD_CH6=10

#for channal seven
declare -i -x YMIN_NUMX_CH7=0
declare -i -x YMAX_NUMX_CH7=3
declare -i -x YMIN_BIAS_CH7=-4
declare -i -x YMAX_BIAS_CH7=10
declare -i -x YMIN_STDD_CH7=0
declare -i -x YMAX_STDD_CH7=10

#for channal eight
declare -i -x YMIN_NUMX_CH8=0
declare -i -x YMAX_NUMX_CH8=3
declare -i -x YMIN_BIAS_CH8=-4
declare -i -x YMAX_BIAS_CH8=10
declare -i -x YMIN_STDD_CH8=0
declare -i -x YMAX_STDD_CH8=10

#for channal nine
declare -i -x YMIN_NUMX_CH9=0
declare -i -x YMAX_NUMX_CH9=3
declare -i -x YMIN_BIAS_CH9=-4
declare -i -x YMAX_BIAS_CH9=10
declare -i -x YMIN_STDD_CH9=0
declare -i -x YMAX_STDD_CH9=10

#for channal ten
declare -i -x YMIN_NUMX_CH10=0
declare -i -x YMAX_NUMX_CH10=3
declare -i -x YMIN_BIAS_CH10=-4
declare -i -x YMAX_BIAS_CH10=10
declare -i -x YMIN_STDD_CH10=0
declare -i -x YMAX_STDD_CH10=10

#for channal eleven
declare -i -x YMIN_NUMX_CH11=0
declare -i -x YMAX_NUMX_CH11=3
declare -i -x YMIN_BIAS_CH11=-4
declare -i -x YMAX_BIAS_CH11=10
declare -i -x YMIN_STDD_CH11=0
declare -i -x YMAX_STDD_CH11=10

#for channal twelve
declare -i -x YMIN_NUMX_CH12=0
declare -i -x YMAX_NUMX_CH12=3
declare -i -x YMIN_BIAS_CH12=-4
declare -i -x YMAX_BIAS_CH12=10
declare -i -x YMIN_STDD_CH12=0
declare -i -x YMAX_STDD_CH12=10

#for channal thirteen
declare -i -x YMIN_NUMX_CH13=0
declare -i -x YMAX_NUMX_CH13=3
declare -i -x YMIN_BIAS_CH13=-4
declare -i -x YMAX_BIAS_CH13=10
declare -i -x YMIN_STDD_CH13=0
declare -i -x YMAX_STDD_CH13=10

#for channal fourteen
declare -i -x YMIN_NUMX_CH14=0
declare -i -x YMAX_NUMX_CH14=3
declare -i -x YMIN_BIAS_CH14=-4
declare -i -x YMAX_BIAS_CH14=10
declare -i -x YMIN_STDD_CH14=0
declare -i -x YMAX_STDD_CH14=10

#for channal fifteem
declare -i -x YMIN_NUMX_CH15=0
declare -i -x YMAX_NUMX_CH15=3
declare -i -x YMIN_BIAS_CH15=-4
declare -i -x YMAX_BIAS_CH15=10
declare -i -x YMIN_STDD_CH15=0
declare -i -x YMAX_STDD_CH15=10

#for channal sixteem
declare -i -x YMIN_NUMX_CH16=0
declare -i -x YMAX_NUMX_CH16=3
declare -i -x YMIN_BIAS_CH16=-4
declare -i -x YMAX_BIAS_CH16=10
declare -i -x YMIN_STDD_CH16=0
declare -i -x YMAX_STDD_CH16=10

#for channal seventeem
declare -i -x YMIN_NUMX_CH17=0
declare -i -x YMAX_NUMX_CH17=3
declare -i -x YMIN_BIAS_CH17=-4
declare -i -x YMAX_BIAS_CH17=10
declare -i -x YMIN_STDD_CH17=0
declare -i -x YMAX_STDD_CH17=10

#for channal eighteem
declare -i -x YMIN_NUMX_CH18=0
declare -i -x YMAX_NUMX_CH18=3
declare -i -x YMIN_BIAS_CH18=-4
declare -i -x YMAX_BIAS_CH18=10
declare -i -x YMIN_STDD_CH18=0
declare -i -x YMAX_STDD_CH18=10

#for channal nineteem
declare -i -x YMIN_NUMX_CH19=0
declare -i -x YMAX_NUMX_CH19=3
declare -i -x YMIN_BIAS_CH19=-4
declare -i -x YMAX_BIAS_CH19=10
declare -i -x YMIN_STDD_CH19=0
declare -i -x YMAX_STDD_CH19=10

#for channal twenty
declare -i -x YMIN_NUMX_CH20=0
declare -i -x YMAX_NUMX_CH20=3
declare -i -x YMIN_BIAS_CH20=-4
declare -i -x YMAX_BIAS_CH20=10
declare -i -x YMIN_STDD_CH20=0
declare -i -x YMAX_STDD_CH20=10
