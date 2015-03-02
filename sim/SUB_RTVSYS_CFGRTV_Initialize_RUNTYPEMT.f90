      SUBROUTINE RTVSYS_CFGRTV_Initialize_RUNTYPEMT( CFGSen, CFGRTV )

! Module use
        USE Type_Kinds  ,       ONLY: DFIP,  DFFP,  DFLP     ! For float
        USE PARAMT_Define 
        USE CFGSen_Define
        USE CFGRTV_Define
 
        IMPLICIT NONE 
 
        TYPE(CFGSen_type),      INTENT(IN)  :: CFGSen
        TYPE(CFGRTV_Type),   INTENT(INOUT)  :: CFGRTV

! 
        INTEGER(KIND=DFIP)       :: NVAR 
        INTEGER(KIND=DFIP)       :: alloc_stat
         
        IF ( PRNAME ) CALL SUB_ENTER('RTVSYS_CFGRTV_Initialize_RUNTYPEMT')

! THE RAD/BTS USED IN READING AND PROCESSING...
        CFGRTV%OINTYPE           = OBSBTS
        CFGRTV%OEXTYPE           = OBSBTS

! FOR FOV
        CFGRTV%N_FOV             = 1
        CFGRTV%FOVTYP            = FOVJMP    ! Jump or CREEP
        CFGRTV%FOVEXE            = FOVIND    ! avg/individual/central

! CHECK THE INPUT/OUTPUT OR NOT.
        CFGRTV%DOCHKINP          = .TRUE.
        CFGRTV%DOCHKOTP          = .TRUE.
        CFGRTV%BNDTYPE           = BNDSIMP 
 
!! FOR THE REFFERENCE DATA, WHICH MATCHED BTS SPATIALLY AND TEMPERALLY
        CFGRTV%DORDREF           = .TRUE.   ! Read REF or Not
        CFGRTV%REFNAME           = DSTCLM  
          
!! FOR NWP/Sonde data
        CFGRTV%DORDNWP           = .TRUE.
        CFGRTV%LDRDNWP           = .FALSE.   ! Read NWP Directly
!        CFGRTV%NWPNAME           = DSTECF
        CFGRTV%NWPNTIM           = 3 
        CFGRTV%COLOCAT           = CLDINV

        CFGRTV%DORDSND           = .FALSE.
        CFGRTV%SNDNSTN           = 2
        CFGRTV%SNDNPLV           = 40
          
        CFGRTV%DORDGAU           = .FALSE.
        CFGRTV%GAUNSTN           = 2
        CFGRTV%GAUNEMS           = 10

        CFGRTV%BNDTYPE           = BNDSIMP        !!!??

! HOWTO DEAL WITH SURFACE EMISSIVITIES.!!!  NOT INCLUDED IN INSPECT..
        CFGRTV%EMSTYPE         = EMSUNT
        CFGRTV%N_EMS           = CFGSEN%N_SEN
        IF ( CFGRTV%CLIMDST == N88CLR .OR. CFGRTV%CLIMDST == SEBCLR .OR. &
             CFGRTV%CLIMDST == SEBCLD ) THEN
          CFGRTV%EMSTYPE         = EMSUNT
          IF ( CFGRTV%CLIMDST == N88CLR ) CFGRTV%N_EMS  = 7
          IF ( CFGRTV%CLIMDST == SEBCLR ) CFGRTV%N_EMS  = 10
          IF ( CFGRTV%CLIMDST == SEBCLD ) CFGRTV%N_EMS  = 10 
        END IF
 
        IF ( PRNAME ) CALL SUB_EXIT('RTVSYS_CFGRTV_Initialize_RUNTYPEMT')
 
      END SUBROUTINE RTVSYS_CFGRTV_Initialize_RUNTYPEMT
 