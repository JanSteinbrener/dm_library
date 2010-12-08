#include "../dm_fileio.h"
#include "../dm_array.h"
#include <math.h>

int main(int argc, char *argv[])
{
  dm_array_complex_struct temp_gaussian;
  dm_array_real_struct gaussian;
  int i,p,my_rank,nx,ny;
  char error_string[128];
  char *filename = "test.png";
  dm_array_real max;

  dm_init(&p,&my_rank);
  
  nx = 1200;
  ny = 1200;
  
  temp_gaussian.nx = nx; 
  temp_gaussian.ny = ny;
  temp_gaussian.nz = 1;
  temp_gaussian.npix = (dm_array_index_t)nx*(dm_array_index_t)ny;
  DM_ARRAY_COMPLEX_STRUCT_INIT((&temp_gaussian),temp_gaussian.npix,p);

  gaussian.nx = nx;
  gaussian.ny = ny;
  gaussian.nz = 1;
  gaussian.npix = (dm_array_index_t)nx*(dm_array_index_t)ny;
  DM_ARRAY_REAL_STRUCT_INIT((&gaussian),gaussian.npix,p);  

  /* store guassian in next_itn_cas_array_struct to save memory */
  dm_array_load_gaussian(&temp_gaussian, 3*3/2,3*3/2,3*3/2, 0, p, my_rank);

  /* Look at magnitudes */
  dm_array_magnitude_complex(&gaussian, &temp_gaussian);

  if (dm_write_png(&gaussian,filename,error_string,0,p,my_rank) 
      == -1) {
    printf("Error writing file: \"%s\"\n",error_string);
  } else {
    printf("Successfully wrote file \"%s\"\n", "test.png");
  }
  
  DM_ARRAY_COMPLEX_FREE(temp_gaussian.complex_array);
  free(gaussian.real_array);
  
  dm_exit();

}
