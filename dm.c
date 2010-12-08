#include "dm.h"

/*------------------------------------------------------------*/
void dm_init(int *p,
             int *this_rank)
{
 
#if !USE_MPI /* We won't use MPI */
  printf("dm_init, no MPI\n");
  *p = 1;
  *this_rank = 0;
#else /* We will use MPI */
  printf("dm_init, use MPI\n");
  MPI_Init(NULL,NULL);
  MPI_Comm_rank(MPI_COMM_WORLD, this_rank);
  MPI_Comm_size(MPI_COMM_WORLD, p);
#endif /*USE_MPI*/
}

/*------------------------------------------------------------*/
  void dm_exit()
{
#if USE_MPI
  MPI_Finalize();
#endif
}

/*------------------------------------------------------------*/
dm_array_real dm_rand(long *idum)
{
#define IA 16807
#define IM 2147483647
#define AM (1.0/IM)
#define IQ 127773
#define IR 2836
#define MASK 123459876

    long k;
    dm_array_real ans;

    *idum ^= MASK;
    k=(*idum)/IQ;
    *idum=IA*(*idum-k*IQ)-IR*k;
    if (*idum < 0) *idum += IM;
    ans = AM*(*idum);
    *idum ^= MASK;

    return((dm_array_real)ans);
}

/*------------------------------------------------------------*/
void dm_time(dm_time_t *this_time)
{
#if USE_MPI
    MPI_Barrier(MPI_COMM_WORLD);
    *this_time = MPI_Wtime();
#else
    *this_time = clock();
#endif
}

/*------------------------------------------------------------*/
double dm_time_diff(dm_time_t start,
		    dm_time_t stop)
{
  double time;
  
#if USE_MPI
  time = stop - start;
#else
  time = ((double)stop - (double)start)/((double)CLOCKS_PER_SEC);
#endif

  return(time);
}

/*------------------------------------------------------------*/
int dm_round(double value)
{
 
  return( (int)(floor(value + 0.5)) );

}

/*------------------------------------------------------------*/
int dm_write_png(dm_array_real_struct *ptr_ras,
		 char *filename,
		 char *error_string,
		 int log_scale,
		 int p,
		 int my_rank)
{
  dm_array_index_t ipix;
  unsigned char *byte_image;
  dm_array_real min, max, temp;
 
  /* write png */
  if (my_rank == 0) {
    
    /* Allocate */ 
    byte_image = (unsigned char *)malloc(ptr_ras->nx*ptr_ras->ny*
					 sizeof(unsigned char));
    
    /* Normalize */
    //max = dm_array_max_real(ptr_ras,p);
    //min = dm_array_min_real(ptr_ras,p);
    max = *(ptr_ras->real_array + 0);
    for (ipix=1;ipix<ptr_ras->npix;ipix++) {
      if (*(ptr_ras->real_array+ipix) > max) {
	max = *(ptr_ras->real_array+ipix);
      }
    }    

    min = *(ptr_ras->real_array + 0);   
    for (ipix=1;ipix<ptr_ras->npix;ipix++) {
      if (*(ptr_ras->real_array+ipix) < min) {
	min = *(ptr_ras->real_array+ipix);
      }
    }    
    

    if (log_scale) {
      /* reset min,max */
      min = 0.0;
      max = (dm_array_real)log10((double)max);
    }
    
    for (ipix=0;ipix<ptr_ras->npix;ipix++) {
      if (log_scale) {
	/* ignore negative values */
	if (*(ptr_ras->real_array + ipix) <= 0) {
	  temp = 0.;
	} else {
	  temp = (dm_array_real)log10((double)*(ptr_ras->real_array + ipix));
	}
      } else {
	temp = *(ptr_ras->real_array + ipix);
      }
      *(byte_image + ipix) = (unsigned char)((temp - min)*255./(max-min));
    }
    
    FILE *fp = fopen(filename,"wb");
    if (!fp) {
      sprintf(error_string,"Unable to open file %s\n",filename);
      free(byte_image);
      return(-1);
    }
    
    png_structp png_ptr = png_create_write_struct
      (PNG_LIBPNG_VER_STRING, (png_voidp)error_string,NULL,NULL);
    if (!png_ptr) {
      free(byte_image);
      return(-1);
    }    
    png_infop info_ptr = png_create_info_struct(png_ptr);
    if (!info_ptr) {
      png_destroy_write_struct(&png_ptr,(png_infopp)NULL);
      free(byte_image);
      return(-1);
    }
    if (setjmp(png_jmpbuf(png_ptr))) {
      png_destroy_write_struct(&png_ptr, &info_ptr);
      fclose(fp);
      free(byte_image);
      return(-1);
    }
    png_init_io(png_ptr, fp);
    png_bytep *row_ptr;
    row_ptr = (png_bytep*)malloc(ptr_ras->ny*sizeof(png_bytep));
    
    for (ipix=0; ipix<ptr_ras->ny; ipix++) row_ptr[ipix]= byte_image+ipix*
				    ptr_ras->nx*sizeof(png_byte);
    png_set_IHDR(png_ptr,info_ptr,ptr_ras->nx,ptr_ras->ny,8,PNG_COLOR_TYPE_GRAY,
		 PNG_INTERLACE_NONE,PNG_COMPRESSION_TYPE_BASE,
		 PNG_FILTER_TYPE_BASE);
    
    png_set_rows(png_ptr,info_ptr,row_ptr);
    png_write_png(png_ptr, info_ptr, PNG_TRANSFORM_IDENTITY, NULL);
    png_write_end(png_ptr, NULL);
    png_destroy_write_struct(&png_ptr, &info_ptr);
    
    free(row_ptr);

    /* free */
    free(byte_image);
  } /* endif(my_rank == 0) */

  return(0);
}
