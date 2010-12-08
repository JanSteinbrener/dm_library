OS	=	$(shell uname -s)
KARCH	=	$(shell uname -m)
NNAME 	=	$(shell uname -n)

# define some combined defines 
DOUBLE = -DDM_ARRAY_DOUBLE -DDIST_FFT_USE_DOUBLE

ifeq ($(OS),Darwin)
	ifeq ($(NNAME),portal2net.cluster.private) # for our cluster at BNL    
		#define ADDITIONAL includes and libraries
		INCLUDE_DIRS = -I/Volumes/RAID/sw/include
		LIB_DIRS = -L/Volumes/RAID/sw/lib

		#define compiler options
		CC = /usr/bin/mpicc
		CFLAGS = -faltivec -03 -mcpu=G5 -mtune=G5 \
			-mpowerpc64 -fstrict-aliasing -g
		LDFLAGS = -bind_at_load

		#define fft libs and dirs
		FFT_LIB = 
		FFT_LIB_DIRS = -L../../dist_fft/
		FFT_INCLUDE_DIRS = -I../../dist_fft/
		FFT_DEFINES = -D__APPLE__ -DDIST_FFT 
                FFT_FRAMEWORK = -framework Accelerate
		FFT_OBJS = dist_fft.o TOMS_transpose.o \
			dist_fft_transpose.o dist_fft_prefetch.o \
			dist_fft_twiddle.o sched.o transpose_mpi.o

		#define HDF5 libs and dirs
		HDF5_INCLUDE_DIR = -I/sw/hdf5/include/ 
		HDF5_LIB_DIR = -L/sw/hdf5/lib/
		HDF5_DEFINES = 
		HDF5_LIB = -lhdf5

		#if applicable, define MPI dirs and libs
		MPI_DEFINES = -D__MPI__
		MPI_LIB = -lmpi
		MPI_LIB_DIR = -L/usr/local/mpi/lib
		MPI_INCLUDE_DIR = -I/usr/local/mpi/include
	else # for any Mac single processor machine
		#define ADDITIONAL includes and libraries
		INCLUDE_DIRS = 
		LIB_DIRS = 

		#define compiler options
		CC = /usr/bin/cc 
		CFLAGS = -g
		LDFLAGS =

		#define fft libs and dirs
		FFT_LIB = -lfftw3f 
		FFT_LIB_DIRS = -L/usr/local/lib
		FFT_INCLUDE_DIRS = -I/usr/local/include
		FFT_DEFINES = 
		FFT_FRAMEWORK = 
		FFT_OBJS = 

		#define HDF5 libs and dirs
		HDF5_INCLUDE_DIR = -I/usr/local/hdf5/include/ 
		HDF5_LIB_DIR = -L/usr/local/hdf5/lib/
		HDF5_DEFINES = 
		HDF5_LIB = -lhdf5

		#if applicable, define MPI dirs and libs
		MPI_DEFINES = 
		MPI_LIB = 
		MPI_LIB_DIR = 
		MPI_INCLUDE_DIR = 
	endif
else # for machines of different OSs
	#define ADDITIONAL includes and libraries
	INCLUDE_DIRS = -I/usr/include -I/usr/local/include
	LIB_DIRS = -L/usr/local/lib -L/usr/lib

	#define compiler options
	CC = /usr/bin/gcc
	CFLAGS = -g
	LDFLAGS = 

	#define fft libs and dirs
	FFT_LIB = -lfftw3f
	FFT_LIB_DIRS = -L/usr/local/lib
	FFT_INCLUDE_DIRS = -I/usr/local/include
	FFT_DEFINES = 
	FFT_FRAMEWORK = 
	FFT_OBJS = 

	#define HDF5 libs and dirs
	HDF5_INCLUDE_DIR = -I/usr/local/hdf5/include/
	HDF5_LIB_DIR = -L/usr/local/hdf5/lib/
	HDF5_DEFINES = 
	HDF5_LIB = -lhdf5

	#if applicable, define MPI dirs and libs
	MPI_DEFINES = 
	MPI_LIB =
	MPI_LIB_DIR = 
	MPI_INCLUDE_DIR = 
endif

# these are common for all
INCL_DIRS_ALL = -I.. -I../.. 
LIBS_ALL = -lpng -lm -lz
FFT_DIR=../../dist_fft/

dm_test_array: dm_test_array.o dm_array.o $(FFT_OBJS) dm.o
	$(CC) $(LDFLAGS) $(FFT_FRAMEWORK) -o dm_test_array \
	dm_test_array.o dm_array.o $(FFT_OBJS) dm.o \
	$(FFT_LIB) $(FFT_LIB_DIRS) $(MPI_LIB) \
	$(MPI_LIB_DIR) $(LIB_DIRS) $(LIBS_ALL)

dm_test_fileio: dm_test_fileio.o dm_fileio.o dm.o
	$(CC) -o dm_test_fileio dm_test_fileio.o dm_fileio.o dm.o \
	$(HDF5_LIB_DIR) $(LIB_DIRS) $(LIBS_ALL) $(FFT_LIB) $(FFT_LIB_DIRS) \
	$(LIB_DIRS) $(HDF5_LIB) $(MPI_LIB) $(MPI_LIB_DIR)

dm_test_write_png: dm_test_write_png.o dm_fileio.o dm_array.o dm.o
	$(CC) -o dm_test_write_png dm_test_write_png.o dm_fileio.o \
	dm_array.o dm.o	$(HDF5_LIB_DIR) $(LIB_DIRS) $(LIBS_ALL) \
	$(FFT_LIB) $(FFT_LIB_DIRS) $(LIB_DIRS) $(HDF5_LIB) \
	$(MPI_LIB) $(MPI_LIB_DIR)

dm_test_array.o: dm_test_array.c ../dm_array.h
	$(CC) -c $(CFLAGS) dm_test_array.c $(FFT_INCLUDE_DIRS) \
	$(INCLUDE_DIRS) $(MPI_INCLUDE_DIR) $(FFT_DEFINES) \
	$(INCL_DIRS_ALL) $(MPI_DEFINES)

dm_array.o: ../dm_array.c ../dm_array.h 
	$(CC) -c $(CFLAGS) ../dm_array.c $(FFT_INCLUDE_DIRS) \
	$(INCLUDE_DIRS)	$(FFT_DEFINES) $(TEST_DEFINES) \
	$(MPI_DEFINES) $(MPI_INCLUDE_DIR) $(INCL_DIRS_ALL)

dist_fft.o: $(FFT_DIR)dist_fft.c $(FFT_DIR)dist_fft.h
	$(CC) -c $(CFLAGS) $(FFT_DIR)dist_fft.c \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES) 

TOMS_transpose.o: $(FFT_DIR)TOMS_transpose.c \
	$(FFT_DIR)TOMS_transpose.h $(FFT_DIR)dist_fft_transpose.h \
	$(FFT_DIR)dist_fft_types.h
	$(CC) -c $(CFLAGS) $(FFT_DIR)TOMS_transpose.c \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES)

dist_fft_transpose.o: $(FFT_DIR)dist_fft_transpose.c \
	$(FFT_DIR)dist_fft_transpose.h \
	$(FFT_DIR)dist_fft_types.h
	$(CC) -c $(CFLAGS) \
	$(FFT_DIR)dist_fft_transpose.c \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES) 

dist_fft_twiddle.o: $(FFT_DIR)dist_fft_twiddle.c \
	$(FFT_DIR)dist_fft_twiddle.h \
	$(FFT_DIR)dist_fft_types.h
	$(CC) -c $(CFLAGS) $(FFT_DIR)dist_fft_twiddle.c \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES)

sched.o: $(FFT_DIR)sched.c $(FFT_DIR)sched.h \
	$(FFT_DIR)transpose_mpi.h $(FFT_DIR)dist_fft_types.h
	$(CC) -c $(CFLAGS) $(FFT_DIR)sched.c \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES)

dist_fft_prefetch.o: $(FFT_DIR)dist_fft_prefetch.c \
	$(FFT_DIR)dist_fft_prefetch.h \
	$(FFT_DIR)dist_fft_twiddle.h $(FFT_DIR)dist_fft_types.h
	$(CC) -c $(CFLAGS) $(FFT_DIR)dist_fft_prefetch.c \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES)

transpose_mpi.o: $(FFT_DIR)transpose_mpi.c $(FFT_DIR)transpose_mpi.h \
	$(FFT_DIR)dist_fft_types.h
	$(CC) -c $(CFLAGS) $(FFT_DIR)transpose_mpi.c \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES)

dm_test_write_png.o: dm_test_write_png.c ../dm_fileio.h ../dm_array.h
	$(CC) -c $(CFLAGS) dm_test_write_png.c $(HDF5_INCLUDE_DIR) \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES) $(HDF5_DEFINES) \
	$(INCLUDE_DIRS) $(MPI_DEFINES) $(MPI_INCLUDE_DIR) $(INCL_DIRS_ALL)

dm_test_fileio.o: dm_test_fileio.c ../dm_fileio.h
	$(CC) -c $(CFLAGS) dm_test_fileio.c $(HDF5_INCLUDE_DIR) \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES) $(HDF5_DEFINES) \
	$(INCLUDE_DIRS) $(MPI_DEFINES) $(MPI_INCLUDE_DIR)

dm_fileio.o: ../dm_fileio.c ../dm_fileio.h
	$(CC) -c $(CFLAGS) ../dm_fileio.c $(HDF5_INCLUDE_DIR) \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES) $(HDF5_DEFINES) \
	$(INCLUDE_DIRS) $(HDF5_LIB) $(MPI_DEFINES) $(MPI_INCLUDE_DIR)

dm.o: ../dm.c ../dm.h
	$(CC) -c $(CFLAGS) ../dm.c $(HDF5_INCLUDE_DIR) \
	$(FFT_INCLUDE_DIRS) $(FFT_DEFINES) $(HDF5_DEFINES) \
	$(INCLUDE_DIRS) $(HDF5_LIB) $(MPI_DEFINES) $(MPI_INCLUDE_DIR) \
	$(LIBS_ALL)

clean:
	rm *.o
