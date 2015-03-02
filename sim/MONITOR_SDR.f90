       PROGRAM MAIN

! Module use
        USE Type_Kinds    ,     ONLY: DFIP,  DFFP,  DFLP
        USE PARAMT_Define
        USE CFGSen_Define
        USE CFGRTV_Define
        USE DATRTV_Define
        USE FILIST_Define

! Disable implicit typing
        IMPLICIT NONE

        TYPE(CFGSen_type)       :: CFGSen
        TYPE(CFGRTV_Type)       :: CFGRTV
        TYPE(DATRTV_Type)       :: DATRTV
 
        CHARACTER(LEN=FILLEN)          :: FIL_IN, FIL_FIX, FIL_OUT, FIL_L1D, CRTMNM
        INTEGER(KIND=DFIP)             :: IREC

        LOGICAL(KIND=DFLP)             :: LTAU, LRAD ,open_flg
 
        INTEGER(KIND=DFIP)             :: IDIN, NLEN, NREC, NBGD, NLZA, ILZA, K, III
        INTEGER(KIND=DFIP)             :: IOS1, IOS2, IOS3, FILUNT, FOUTUNT
 
        LOGICAL(KIND=DFLP), PARAMETER  :: LTEST = .FALSE.
        INTEGER(KIND=DFIP)             :: IYMD, ISEC, NSLN, IYER, IMON, IDAY, IHOR, IMUN
        LOGICAL(KIND=DFLP)             :: LEXIST
        INTEGER(KIND=DFIP)             :: IEXE, NEXE , ind
        INTEGER(KIND=DFIP)             :: L1DOUT(1000)
        INTEGER  Block, IREC_Num, OrderID                
! EXTERNAL
        REAL(KIND=DFFP)                :: SYSANC_GEOTIM_YMDHM2TIM

!Tmp mem of outdata
        TYPE :: DATA_OUT
                INTEGER(KIND=DFIP)             :: IREC,Length 
                INTEGER(KIND=DFIP)             :: L1DOUT(1000)
        END TYPE DATA_OUT

        TYPE(DATA_OUT), allocatable :: outdata(:)

! Input-Parameter
        Integer*2     n_Static, n_Command_Line
        Character*10  arg
!! Read OrderID from input-Parameter
        n_Command_Line=1
        Call Getarg(n_Command_Line, arg, n_Static)
        READ(arg,'(I3)') OrderID
!        print *,"OrderID=",OrderID

!! Read in Setting Values
        CFGRTV%FIL_LST = "Monitor_SDR_filist.txt"
        CALL SETNML_Initialize( CFGRTV%FIL_LST )
!        WRITE(*, NML=SETNML)
        !STOP

        IF ( TRIM(Sen_NAME) == "NTST" )  Sen_NAME  = "FY3BMWTS"  ! "FY4AGIRS"   ! "FY3BVASS"   ! 
        IF ( TRIM(Sen_NAME) == "FY3BVASS" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 35
! FY3C
        IF ( TRIM(Sen_NAME) == "FY3CVASS" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 54
        IF ( TRIM(Sen_NAME) == "FY3CMWTS" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 13
        IF ( TRIM(Sen_NAME) == "FY3CMWHS" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 15
        IF ( TRIM(Sen_NAME) == "FY3CMWRI" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 10
        IF ( TRIM(Sen_NAME) == "FY3CIRAS" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 26


        IF ( TRIM(Sen_NAME) == "FY4AGIRS" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 913 

        IF ( TRIM(Sen_NAME) == "NA18TOVS" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 40
        IF ( TRIM(Sen_NAME) == "NA18MSUA" .AND. SEN_NCHN == IFILVL ) SEN_NCHN  = 15

        CRTMNM  = TRIM(RTM_NAME)  !! FOR OUTPUT
        IF ( TRIM(RTM_NAME) == "CRTM202"  )  RTM_NAME  = "CRTMv20x"
        IF ( TRIM(RTM_NAME) == "RTTOV101" )  RTM_NAME  = "RTTOVv10x" 
 
        CFGSen%DIR_ROOT         = TRIM(DIR_ROOT)
        CFGSen%DIR_MAIN         = TRIM(DIR_MAIN)

        CFGSen%n_chn            = SEN_NCHN
        CFGSen%n_lev            = RTM_NPLV
        CFGSen%Sen_name         = TRIM(ADJUSTL(Sen_NAME))
        CFGSen%RTM_NAME         = TRIM(ADJUSTL(RTM_NAME))
        CALL RTVSYS_CFGSen_Initialize( CFGSen ) 
!        IF ( INSPKT ) CALL CFGSen_Inspect( CFGSen ) 

! Specify information on retrieval method
        CFGRTV%RUNTYPE          = RUNTMT
        
!!!! FOR THE NAME AND FMT OF L1D FILES
        IF ( TRIM(L1D_NAME) == "DSTSAT" ) CFGRTV%L1DNAME   = DSTSAT ! SEBCLR ! SEBCLD ! BJX721 ! DSTSAT !
        IF ( TRIM(L1D_NAME) == "SEBCLR" ) CFGRTV%L1DNAME   = SEBCLR
        IF ( TRIM(L1D_NAME) == "SEBCLD" ) CFGRTV%L1DNAME   = SEBCLD
        IF ( TRIM(L1D_NAME) == "N88CLR" ) CFGRTV%L1DNAME   = N88CLR
        
        IF ( TRIM(L1D_FMTX) == "NSMCSLF") CFGRTV%L1DFMTX   = NSMCSLF
        IF ( TRIM(L1D_FMTX) == "TOVSL1X") CFGRTV%L1DFMTX   = TOVSL1X
        IF ( TRIM(L1D_FMTX) == "CLMXSLF") CFGRTV%L1DFMTX   = CLMXSLF                
                
!!       CFGRTV%EMSNAME          = N88CLR    !!! MUST BE COMMENTED
        IF ( TRIM(NWP_NAME) == "T639" ) CFGRTV%NWPNAME = DST639
        IF ( TRIM(NWP_NAME) == "NFNL" ) CFGRTV%NWPNAME = DSTFNL
        IF ( TRIM(NWP_NAME) == "ERAI" ) CFGRTV%NWPNAME = DSTERI
        
        !print *, CFGRTV%L1DNAME, CFGRTV%L1DFMTX, TOVSL1X, CFGRTV%NWPNAME, DSTFNL
        !stop
        
        CALL RTVSYS_CFGRTV_Initialize ( CFGSen, CFGRTV )
!        IF ( INSPKT ) CALL CFGRTV_Inspect( CFGSen, CFGRTV )
 
        WRITE(CFGSen%DIR_DIN1, "(2a)" ) TRIM(CFGSen%DIR_ROOT), "/data"
        WRITE(CFGSen%DIR_DIN2, "(2a)" ) TRIM(CFGSen%DIR_ROOT), "/../FYSATDATA"
 
        CALL RTVSYS_DATRTV_Initialize( CFGSen, CFGRTV, DATRTV )
!        IF ( INSPKT ) CALL DATRTV_Inspect ( CFGSen, CFGRTV, DATRTV )
         
        DO K = 1, MXNVAR
          DATRTV%CRIDNED(K)  = K
        END DO
 
        CALL RTVSYS_RTMDEP_INITIALIZE( CFGSen, CFGRTV, DATRTV )
        CALL RTVSYS_LOADANC( CFGSen, CFGRTV, DATRTV )
        LTAU   = .FALSE. 
        LRAD   = .FALSE.
 
!! FILE CONTAINS L1D-FILE INFORMATION
        WRITE(FIL_IN, '(A)')  TRIM(FIL_OBSL1D)
        INQUIRE( FILE=TRIM(FIL_IN), EXIST=LEXIST )
        IF( .NOT. LEXIST ) THEN
          WRITE(FIL_IN, '(3A)') TRIM(FIL_OBSL1D)
        END IF
        FILUNT  = IIL_OBSL1D(1)
        OPEN( FILUNT, FILE=TRIM(FIL_IN), IOSTAT=IOS1 )
!        PRINT *, "  L1D: ", TRIM(FIL_IN), IOS1  
              
!! for FWD       
        OPEN( IIL_FWDBTS(1), FILE=TRIM(FIL_FWDBTS), IOSTAT=IOS3)
!        PRINT *, "FWD: ", TRIM(FIL_FWDBTS), IOS3
 
!        DO WHILE( IOS1 == 0 )
          READ( FILUNT, "(i9, i5, i9, a)", IOSTAT=IOS1 ) IYMD, ISEC, NSLN, FIL_L1D
!          print *, "hello..1..", IYMD, ISEC, NSLN, trim(FIL_L1D)
!          IF ( IOS1 /= 0 ) EXIT
          IF ( IOS1 /= 0 ) STOP
          DATRTV%DATL1D(:)%FILL1D  = TRIM( FIL_L1D )
          DATRTV%DATL1D(:)%FILUNT  = FILUNT*10 + 1
          DATRTV%DATL1D(:)%NSLN    = NSLN  
          CFGSen%N_SLN             = NSLN 
 
          READ(IIL_FWDBTS(1), "(i9, i5, i9, a)" ) IYMD, ISEC, NSLN, FIL_FIX 
          print *, "hello..2..",IYMD, ISEC, NSLN, trim(FIL_FIX), DATRTV%DATL1D(1)%FILLEN
          WRITE(FIL_OUT, "(8a)") TRIM(FIL_FIX), "_FWDBTS_", TRIM(NWP_NAME), "_", TRIM(CRTMNM),  &
                               "_", TRIM(L1D_FMTX), ".bin"
!          print *, TRIM(FIL_OUT)
!          stop
          FOUTUNT   = IIL_FWDBTS(1)*10 + 1
!          OPEN (FOUTUNT, FILE=TRIM(FIL_OUT), RECL=DATRTV%DATL1D(1)%FILLEN*4,  ACCESS='DIRECT', IOSTAT=IOS3 )
          !PRINT *, "FIL_OUT: ", TRIM(FIL_OUT)
          !PRINT *, "UNIT: ", FOUTUNT, "LEN: ", DATRTV%DATL1D(1)%FILLEN

!! Time of the beginning of this grannule.
          DATRTV%DATGEO%YEAR   = IYMD/10000
          DATRTV%DATGEO%MONTH  = MOD( IYMD, 10000)/100
          DATRTV%DATGEO%DAY    = MOD( IYMD, 100)
          DATRTV%DATGEO%HOUR   = ISEC/100
          DATRTV%DATGEO%MINU   = MOD(ISEC, 100)
          DATRTV%DATGEO%TIM    = SYSANC_GEOTIM_YMDHM2TIM( DATRTV%DATGEO%YEAR, DATRTV%DATGEO%MONTH,   &
                                       DATRTV%DATGEO%DAY, DATRTV%DATGEO%HOUR, DATRTV%DATGEO%MINU ) 
!!! LOAD NWP DATA
         CALL RTVSYS_LOADNWP( CFGSen, CFGRTV, DATRTV, DATRTV%DATGEO, DATRTV%DATNWP )

         IREC  = 0; IOS2  = 0

         !!change by yuanwt 
!         IREC_Num = 0
!         DO WHILE( IOS2 == 0 )
!            IREC_Num  = IREC_Num  + 1
!            CALL RTVSYS_LOADL1D( IREC_Num, CFGSen, CFGRTV, DATRTV, IOS2)
!            IF ( IOS2 /= 0 )  exit
!         END DO
!! ABOVE: COMMENTED OUT BY WUCQ
         IREC_Num = CFGSen%N_SLN * CFGSen%N_SPT
         Block=IREC_Num/30  + 1

         Allocate(outdata(Block))

         IOS2  = 0
         !IREC = OrderID*Block
         ind = 1
         !DO WHILE( IOS2 == 0 )
         DO IREC = OrderID*Block + 1 , (OrderID+1)*Block
         !   IREC  = IREC + 1
            IF( MOD(IREC, 10000) == 1 ) PRINT *, "IREC: ", IREC

            CALL RTVSYS_LOADL1D( IREC, CFGSen, CFGRTV, DATRTV, IOS2 )
            IF ( IOS2 /= 0 ) EXIT
            !PRINT *, "IREC: ", IREC
            
            NEXE  = 1
            IF ( CFGRTV%FOVEXE == FOVIND ) NEXE = CFGRTV%N_FOV
       
            DO IEXE = 1, NEXE
              CALL RTVSYS_L1D2BTS( CFGSEN, CFGRTV, DATRTV, IEXE, DATRTV%DATGEO, DATRTV%BTSOBS )
!              PRINT *, "OBS: ", SNGL( DATRTV%BTSOBS%ORIBTS ) 
!              PRINT *, SNGL(DATRTV%DATGEO%LON),  SNGL(DATRTV%DATGEO%LAT), SNGL(DATRTV%DATGEO%LZA), SNGL(DATRTV%DATGEO%LAA), &
!                       SNGL(DATRTV%DATGEO%LSA),  SNGL(DATRTV%DATGEO%SZA), SNGL(DATRTV%DATGEO%SAA), SNGL(DATRTV%DATGEO%YEAR), &
!                       SNGL(DATRTV%DATGEO%MONTH),  SNGL(DATRTV%DATGEO%DAY), SNGL(DATRTV%DATGEO%HOUR), SNGL(DATRTV%DATGEO%MINU)
!              pause 
!!! REMAP NWP DATA TO SAT FOV 
  
              CALL RTVSYS_NWP2PIXEL( CFGSEN, CFGRTV, DATRTV, DATRTV%DATGEO, DATRTV%DATNWP, DATRTV%ATMOBS )
              DATRTV%ATMOBS%ABS_IDS  = CFGSen%ABS_IDS
 
!!! CALCULATE BTS            
              WHERE( ABS(CFGSen%Sen_chan) > 0 ) 
                CFGRTV%ADJC_KUSE = 1
              END WHERE
              CFGRTV%ADJC_NUSE   = COUNT( CFGRTV%ADJC_KUSE > 0 )
!              PRINT *, DATRTV%DATGEO%LZA, DATRTV%DATGEO%LSA, DATRTV%DATGEO%LAA, DATRTV%DATGEO%SAA, DATRTV%DATGEO%SZA
!              PRINT *, DATRTV%DATGEO%badgeo
!              STOP
              IF( DATRTV%DATGEO%BADGEO ) THEN
                DATRTV%BTSOBS%FWDBTS   = RFILVL
              ELSE
                CALL RTVSYS_FWDBT(CFGSEN, CFGRTV, DATRTV, DATRTV%ATMOBS, LRAD, LTAU)
              END IF
              
!              print *, sngl( DATRTV%BTSOBS%FWDBTS )
!              stop
!! SET VARS TO L1D FORMATTED RECORD
              CALL SYSANC_L1DOUT(CFGSEN, CFGRTV, DATRTV, DATRTV%DATGEO, DATRTV%BTSOBS, DATRTV%DATL1D(1), L1DOUT) 
!              PRINT *, L1DOUT(:DATRTV%DATL1D(1)%FILLEN) 
!              stop
!              PRINT *, SNGL(L1DOUT(:DATRTV%DATL1D(1)%FILLEN)), DATRTV%DATNWP%FIL_TIM(DATRTV%DATNWP%IFILS) , sngl(DATRTV%DATNWP%COEFS)
             
               outdata(ind)%IREC = IREC
               outdata(ind)%Length = DATRTV%DATL1D(1)%FILLEN
               outdata(ind)%L1DOUT(:DATRTV%DATL1D(1)%FILLEN) = L1DOUT(:DATRTV%DATL1D(1)%FILLEN)

            END DO     !! IEXE
            ind = ind + 1
            
          END DO       !! L1D RECS IN ONE FILE

              inquire(file=TRIM(FIL_OUT),opened=open_flg)
              DO WHILE( open_flg )
                   inquire(file=TRIM(FIL_OUT),opened=open_flg)
              END DO
              
              !gumeng. add add  4 nwp data of float: nwp_begin_time, nwp_end_time, nwp_begin_coef, nwp_end_coef.
              OPEN (FOUTUNT, FILE=TRIM(FIL_OUT), RECL=(DATRTV%DATL1D(1)%FILLEN+6)*4,  ACCESS='DIRECT', IOSTAT=IOS3 )
              
!              OPEN (FOUTUNT, FILE=TRIM(FIL_OUT), RECL=DATRTV%DATL1D(1)%FILLEN*4,  ACCESS='DIRECT', IOSTAT=IOS3 )
!              print *,"OrderID=",OrderID
!              print *,"Block=",Block
!              print *,"IREC - OrderID*Block=",IREC - OrderID*Block

              DO ind=1, IREC - OrderID*Block  - 1
                !print *, outdata(ind)%IREC
                !print *, outdata(ind)%Length
                !print *, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@"
                
                ! gumeng. 
                WRITE(FOUTUNT, REC=outdata(ind)%IREC ) outdata(ind)%L1DOUT(:outdata(ind)%Length) , DATRTV%DATNWP%FIL_TIM(DATRTV%DATNWP%IFILS), sngl(DATRTV%DATNWP%COEFS)
!                WRITE(FOUTUNT, REC=outdata(ind)%IREC ) outdata(ind)%L1DOUT(:outdata(ind)%Length) !, sngl( DATRTV%DATNWP%FIL_TIM(DATRTV%DATNWP%IFILS)), sngl(DATRTV%DATNWP%COEFS)
              END DO
 
!        END DO         !! L1D FILES
 
        CALL RTVSYS_DATRTV_Terminate( CFGSen, CFGRTV,  DATRTV )
        CALL RTVSYS_CFGRTV_Terminate( CFGRTV )
        CALL RTVSYS_CFGSen_Terminate( CFGSen )
  
      END PROGRAM MAIN
 
