#include "../dm.h"
#include "../dm_fileio.h"
#include <time.h>

/* Longer strings needed for the comments */
#define STRLEN 128
#define MAX_COMMENT_STRINGS 16

/* Different string length for ainfo_struct to save some 
 * memory since filenames tend to be short. 
 * NOTE: file-directory might need a longer string.
 */
#define MAX_FRAMES 128
#define AINFO_STRLEN 64


void dm_test_fileio_help();

int main(int argc, char *argv[]) {
  char filename[STRLEN], this_arg[STRLEN], error_string[STRLEN];
  char csv_filename[STRLEN];
  dm_adi_struct my_adi_struct;
  dm_ainfo_struct my_ainfo_struct;
  dm_spt_struct my_spt_struct;
  dm_itn_struct my_itn_struct;
  dm_array_real_struct my_adi_array_struct, my_adi_error_array_struct;
  dm_array_real_struct recon_errors;
  dm_array_complex_struct my_itn_array_struct;
  dm_array_byte_struct my_spt_array_struct;
  dm_comment_struct my_comment_struct;
  hid_t h5_file_id;
  int n_dims, i_arg, is_readonly, make_error, error_is_present;
  int ix, iy, iz, nx, ny, nz, half_nx, half_ny, half_nz;
  int recon_errors_npix;
  int xmax,ymax,zmax;
  int adi_array_allocated, adi_error_array_allocated;
  int spt_array_allocated, itn_array_allocated, comstr_allocated;
  int ainfo_allocated, status;
  int recon_errors_allocated;
  dm_array_index_t i;
  int n_strings, n_frames, string_length;
  double temp_double;
  time_t t;
  int my_rank,p;
  int DebugWait,print_limit;
  
  dm_init(&p,&my_rank);

  /* hardwired defaults */
  print_limit = 5;
  i_arg = 1;
  
  /* defaults that user can modify through CLAs */
  make_error = 0;
  comstr_allocated = 0;
  adi_array_allocated = 0;
  adi_error_array_allocated = 0;
  spt_array_allocated = 0;
  itn_array_allocated = 0;
  ainfo_allocated = 0;
  recon_errors_allocated = 0;
  is_readonly = 1;
  n_dims = 2;
  DebugWait = 0;

  while (i_arg < argc) {
    strcpy( this_arg, argv[i_arg] );
    if ((strncasecmp("-?",this_arg,2) == 0) || 
	(strncasecmp("-H",this_arg,2) == 0)) {
      dm_test_fileio_help();
      exit(1);
    } else if (strncasecmp("-R",this_arg,2) == 0) {
      is_readonly = 1;
      i_arg++;
    } else if (strncasecmp("-D",this_arg,2) == 0) {
      DebugWait = 1;
      i_arg++;
    } else if (strncasecmp("-W",this_arg,2) == 0) {
      is_readonly = 0;
      i_arg++;
    } else if (strncasecmp("-U",this_arg,2) == 0) {
      is_readonly = 2;
      i_arg++;
    } else if (strncasecmp("-E",this_arg,2) == 0) {
      make_error = 1;
      i_arg++;
    } else if (strncasecmp("-N",this_arg,2) == 0) {
      sscanf(argv[i_arg+1],"%d",&n_dims);
      i_arg = i_arg+2;
    } else {
      strcpy(filename,this_arg);
      i_arg++;
    }
  }

  while (DebugWait);
  
#ifdef DM_ARRAY_DOUBLE
  printf("Using dm_array_real=double\n");
#else
  printf("Using dm_array_real=float\n");
#endif

  if (is_readonly == 0) {
    /* This is how we initialize dm_comment_struct */
    my_comment_struct.n_strings_max = MAX_COMMENT_STRINGS;
    my_comment_struct.string_length = STRLEN;
    my_comment_struct.string_array = 
      (char *)malloc(my_comment_struct.n_strings_max*
		     my_comment_struct.string_length);
    my_comment_struct.specimen_name = 
      (char *)malloc(my_comment_struct.string_length);
    my_comment_struct.collection_date = 
      (char *)malloc(my_comment_struct.string_length);
    comstr_allocated = 1;
    dm_clear_comments(&my_comment_struct);
    
    dm_add_string_to_comments("This is the first comment line.",
			      &my_comment_struct);
    dm_add_string_to_comments("This is something equally useless.",
			      &my_comment_struct);
    dm_add_string_to_comments("Now this is getting ridiculous!",
			      &my_comment_struct);
    dm_add_string_to_comments("Let's get this over with already...",
			      &my_comment_struct);
    dm_add_specimen_name_to_comments("Yeast cell",&my_comment_struct);
    time(&t);
    dm_add_collection_date_to_comments(ctime(&t),&my_comment_struct);

    /* Similar for our ainfo structure */
    my_ainfo_struct.n_frames_max = MAX_FRAMES;
    my_ainfo_struct.string_length = AINFO_STRLEN;
    my_ainfo_struct.ainfo_tags = AINFO_TAGS;
    
    /* Use comment string length for file directory */
    my_ainfo_struct.file_directory =
      (char *)malloc(my_ainfo_struct.string_length);    
    my_ainfo_struct.filename_array = 
      (char *)malloc(my_ainfo_struct.n_frames_max*
		     my_ainfo_struct.string_length);
    my_ainfo_struct.systime_array = 
      (char *)malloc(my_ainfo_struct.n_frames_max*
		     my_ainfo_struct.string_length);
    my_ainfo_struct.theta_x_radians_array = 
      (double *)malloc(my_ainfo_struct.n_frames_max*sizeof(double));
    my_ainfo_struct.xcenter_offset_pixels_array = 
      (double *)malloc(my_ainfo_struct.n_frames_max*sizeof(double));
    my_ainfo_struct.ycenter_offset_pixels_array = 
      (double *)malloc(my_ainfo_struct.n_frames_max*sizeof(double)); 
    ainfo_allocated = 1;
    /* Important to clear the structure first!!! */
    dm_clear_ainfo(&my_ainfo_struct);

    /* For now we will manually adjust the n_frames variable */
    my_ainfo_struct.n_frames = 5;
    
    dm_add_file_directory_to_ainfo("/users/local/data/nov2005/",
      &my_ainfo_struct); 
    

    /*strcpycsv_filename,"/Users/jansteinbrenner/Desktop/testfile.mg");
    if (dm_read_ainfo_from_csv(csv_filename, &my_ainfo_struct,
			       error_string) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      exit(1);  
      } */

    /* Don't specify all entries to see if dm_check_ainfo works */
    for (iy=0; iy<my_ainfo_struct.n_frames-2; iy++) {
      dm_add_systime_to_ainfo("today", &my_ainfo_struct);
      if (dm_add_double_to_ainfo("xcenter",-0.5, &my_ainfo_struct,
				 error_string) != DM_FILEIO_SUCCESS) {
	printf("%s\n",error_string);
	exit(1);
      }
      if (dm_add_double_to_ainfo("ycenter",-1, &my_ainfo_struct,
				 error_string) != DM_FILEIO_SUCCESS) {
	printf("%s\n",error_string);
	exit(1);
      }
      
    }
    my_ainfo_struct.no_background = 1;
    my_ainfo_struct.merge_first = 0;
    my_ainfo_struct.dk_by_pix = 0;
   

    my_adi_struct.photon_scaling = 1.;
    my_adi_struct.error_scaling = 2.;
    my_adi_struct.lambda_meters = 2.5e-9;
    my_adi_struct.camera_z_meters = 0.15;
    my_adi_struct.camera_x_pixelsize_meters = 20.e-6;
    my_adi_struct.camera_y_pixelsize_meters = 21.e-6;
    my_adi_struct.camera_alpha_radians = .9;
    my_adi_struct.camera_beta_radians = 1.9;
    my_adi_struct.camera_gamma_radians = 3.0;
    my_adi_struct.median_filter_width = 20;
    my_adi_struct.median_filter_threshold = .5;
    my_adi_struct.saturation_min = .2;
    my_adi_struct.saturation_max = .9;
    my_adi_struct.theta_x_radians = 1.;
    my_adi_struct.theta_y_radians = 2.;
    my_adi_struct.theta_z_radians = 3.;
    my_adi_struct.xcenter_offset_pixels = 1.5;
    my_adi_struct.ycenter_offset_pixels = 2.5; 

    my_spt_struct.support_scaling = 1.;
    my_spt_struct.pix_x_meters = 20.e-9;
    my_spt_struct.pix_y_meters = 20.e-9;
    my_spt_struct.pix_z_meters = 20.e-9;

    my_itn_struct.photon_scaling = 1.;
    my_itn_struct.pix_x_meters = 20.e-9;
    my_itn_struct.pix_y_meters = 20.e-9;
    my_itn_struct.pix_z_meters = 20.e-9;
    my_itn_struct.iterate_count = 30;

    /* Make a ramp pattern */
    sprintf(filename,"ramp_%dd",n_dims);
    my_adi_array_struct.nx = 512;
    my_adi_array_struct.ny = 512;
    if (n_dims == 3) {
      my_adi_array_struct.nz = 512;
    } else {
      my_adi_array_struct.nz = 1;
    }

    /* We need this in case we use MPI */
    if (n_dims == 1) {
        xmax = my_adi_array_struct.nx/p;
        ymax = my_adi_array_struct.ny;
        zmax = my_adi_array_struct.nz;
    } else if (n_dims == 2) {
        xmax = my_adi_array_struct.nx;
        ymax = my_adi_array_struct.ny/p;
        zmax = my_adi_array_struct.nz;
    } else if (n_dims == 3) {
        xmax = my_adi_array_struct.nx;
        ymax = my_adi_array_struct.ny;
        zmax = my_adi_array_struct.nz/p;
    }
    my_adi_array_struct.npix = 
      (dm_array_index_t)my_adi_array_struct.nx*
      (dm_array_index_t)my_adi_array_struct.ny*
      (dm_array_index_t)my_adi_array_struct.nz;
    DM_ARRAY_REAL_STRUCT_INIT((&my_adi_array_struct),
                              my_adi_array_struct.npix,p);

    adi_array_allocated = 1;

    my_spt_array_struct.nx = my_adi_array_struct.nx;
    my_spt_array_struct.ny = my_adi_array_struct.ny;
    my_spt_array_struct.nz = my_adi_array_struct.nz;
    my_spt_array_struct.npix = my_adi_array_struct.npix;
    DM_ARRAY_BYTE_STRUCT_INIT((&my_spt_array_struct),
                              my_spt_array_struct.npix,p);

    spt_array_allocated = 1;
    
    my_itn_array_struct.nx = my_adi_array_struct.nx;
    my_itn_array_struct.ny = my_adi_array_struct.ny;
    my_itn_array_struct.nz = my_adi_array_struct.nz;
    my_itn_array_struct.npix = my_adi_array_struct.npix;
    DM_ARRAY_COMPLEX_STRUCT_INIT((&my_itn_array_struct),
                                 my_itn_array_struct.npix,p);
    itn_array_allocated = 1;

    if (my_rank == 0) {
        recon_errors.nx = my_itn_struct.iterate_count;
        recon_errors.ny = 1;
        recon_errors.nz = 1;
        recon_errors.npix = recon_errors.nx;
        /* Recon errors will not be split up over the nodes 
         * so p = 1 in this case.
         */
        recon_errors.real_array = 
            (dm_array_real *)malloc(recon_errors.npix*sizeof(dm_array_real));
        recon_errors_allocated = 1;
    }

    for (iz=0; iz<zmax; iz++) {
      for (iy=0; iy<ymax; iy++) {
	for (ix=0; ix<xmax; ix++) {
	  i = (ix+iy*my_adi_array_struct.nx+
	       iz*my_adi_array_struct.nx*my_adi_array_struct.ny);

	  *(my_adi_array_struct.real_array+i) = (dm_array_real)i;

	  *(my_spt_array_struct.byte_array+i) = (i % 256);

	  c_re(my_itn_array_struct.complex_array,i) = 
              (dm_array_real)i*(my_rank+1);
	  c_im(my_itn_array_struct.complex_array,i) = 
              (dm_array_real)0.5*i*(my_rank+1);

	}
      }
    }
    
    if (my_rank == 0) {
      for (ix=0;ix<recon_errors.npix;ix++) {
	*(recon_errors.real_array + ix) = 
	  (dm_array_real)1./(dm_array_real)(ix+1);
      }
    }

    if (make_error == 1) {
      printf("Adding an error array\n");
      my_adi_error_array_struct.nx = my_adi_array_struct.nx;
      my_adi_error_array_struct.ny = my_adi_array_struct.ny;
      my_adi_error_array_struct.nz = my_adi_array_struct.nz;
      my_adi_error_array_struct.npix = my_adi_array_struct.npix;
      DM_ARRAY_REAL_STRUCT_INIT((&my_adi_array_struct),
                                my_adi_array_struct.npix,p);
      adi_error_array_allocated = 1;
      
      for (iz=0; iz<zmax; iz++) {
	for (iy=0; iy<ymax; iy++) {
	  for (ix=0; ix<xmax; ix++) {
	    i = (ix+iy*my_adi_error_array_struct.nx+
		 iz*my_adi_error_array_struct.nx*my_adi_error_array_struct.ny);
	    *(my_adi_error_array_struct.real_array+i) = 1.;
	  }
	}
      }
    } else {
      my_adi_error_array_struct.nx = 0;
      my_adi_error_array_struct.ny = 0;
      my_adi_error_array_struct.nz = 0;
      my_adi_error_array_struct.npix = 0;
      my_adi_error_array_struct.real_array = NULL;
    }
  
#ifdef DM_ARRAY_DOUBLE
    strcat(filename,"_double");
#else
    strcat(filename,"_float");
#endif
#if (BYTE_ORDER == LITTLE_ENDIAN)
    strcat(filename,"_le.h5");
#else
    strcat(filename,"_be.h5");
#endif
    if (dm_h5_create(filename,&h5_file_id,
		     error_string,my_rank) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      exit(1);
    }
    if (dm_h5_create_comments(h5_file_id,&my_comment_struct,
			      error_string,my_rank) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }

    if (dm_h5_write_adi(h5_file_id,&my_adi_struct,
			&my_adi_array_struct,&my_adi_error_array_struct,
			error_string,my_rank,p) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }
    
    if (dm_h5_write_ainfo(h5_file_id,&my_ainfo_struct,
			  error_string,my_rank) != DM_FILEIO_SUCCESS) {
        printf("%s\n",error_string);
        dm_h5_close(h5_file_id,my_rank);
        exit(1);
    }
    
    if (dm_h5_write_spt(h5_file_id,&my_spt_struct,
			&my_spt_array_struct,
			error_string,my_rank,p) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }

    if (dm_h5_write_itn(h5_file_id,&my_itn_struct,
			&my_itn_array_struct,&recon_errors,
			error_string,my_rank,p) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }
    dm_h5_close(h5_file_id,my_rank);
    printf("Wrote file \"%s\"\n", filename);
    
  } else if (is_readonly == 1) {
    /* OK, in this case we are going to read in a file */
    if (strlen(filename) == 0) {
      dm_test_fileio_help();
      exit(1);
    }
    
    printf("Opening file \"%s\"\n",filename);
    if (dm_h5_openwrite(filename,&h5_file_id,error_string,my_rank) 
	!= DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      exit(1);
    }
    
    if (dm_h5_read_comments_info(h5_file_id,&n_strings,&string_length,
				 error_string,my_rank) != DM_FILEIO_SUCCESS) {
        printf("%s\n",error_string);
        dm_h5_close(h5_file_id,my_rank);
        exit(1);
    }

    /* This is how we initialize dm_comment_struct */
    my_comment_struct.n_strings_max = n_strings;
    my_comment_struct.string_length = string_length;
    printf("Comment strings: %d. String length: %d\n",n_strings,string_length);
    if (comstr_allocated == 1) {
        free(my_comment_struct.string_array);
        free(my_comment_struct.specimen_name);
        free(my_comment_struct.collection_date);
        DM_ARRAY_COMPLEX_FREE(my_itn_array_struct.complex_array);
    }
    my_comment_struct.string_array = 
        (char *)malloc(my_comment_struct.n_strings_max*
                       my_comment_struct.string_length);
    my_comment_struct.specimen_name = 
        (char *)malloc(my_comment_struct.string_length);
    my_comment_struct.collection_date = 
        (char *)malloc(my_comment_struct.string_length);
    comstr_allocated = 1;
    dm_clear_comments(&my_comment_struct);
    if (dm_h5_read_comments(h5_file_id,&my_comment_struct,
                            error_string,my_rank) != DM_FILEIO_SUCCESS) {
        printf("%s\n",error_string);
        dm_h5_close(h5_file_id,my_rank);
        exit(1);
    }
    printf("Comments:\n");
    dm_print_comments(stdout," ",&my_comment_struct,"\n",my_rank);
    
    /*To create dm_ainfo_struct, first determine its size
      if (dm_h5_read_ainfo_info(h5_file_id,&n_frames,&string_length,
      error_string,my_rank) != DM_FILEIO_SUCCESS) {
        printf("%s\n",error_string);
        dm_h5_close(h5_file_id,my_rank);
        exit(1);
    }

    my_ainfo_struct.n_frames_max = n_frames;
    my_ainfo_struct.string_length = string_length;
    if (ainfo_allocated) {
        free(my_ainfo_struct.filename_array);
        free(my_ainfo_struct.file_directory);
        free(my_ainfo_struct.systime_array);
        free(my_ainfo_struct.theta_x_radians_array);
        free(my_ainfo_struct.xcenter_offset_pixels_array);
        free(my_ainfo_struct.ycenter_offset_pixels_array);
    }
    my_ainfo_struct.filename_array = 
        (char *)malloc(my_ainfo_struct.n_frames_max*
                       my_ainfo_struct.string_length);
    my_ainfo_struct.file_directory = 
        (char *)malloc(my_ainfo_struct.string_length);
    my_ainfo_struct.systime_array = 
        (char *)malloc(my_ainfo_struct.n_frames_max*
                       my_ainfo_struct.string_length);
    my_ainfo_struct.theta_x_radians_array = 
        (double *)malloc(my_ainfo_struct.n_frames_max*sizeof(double));
    my_ainfo_struct.xcenter_offset_pixels_array = 
        (double *)malloc(my_ainfo_struct.n_frames_max*sizeof(double));
    my_ainfo_struct.ycenter_offset_pixels_array = 
        (double *)malloc(my_ainfo_struct.n_frames_max*sizeof(double)); 
    ainfo_allocated = 1;
    dm_clear_ainfo(&my_ainfo_struct);
    if (dm_h5_read_ainfo(h5_file_id, &my_ainfo_struct,
                         error_string,my_rank) != DM_FILEIO_SUCCESS) {
        printf("%s\n",error_string);
        dm_h5_close(h5_file_id,my_rank);
        exit(1);
    }
    printf("Assembly info:\n");
    dm_print_ainfo(stdout," ",&my_ainfo_struct,",",my_rank);*/
    
    /*Reading the /adi group */

    if (dm_h5_adi_group_exists(h5_file_id,my_rank) == 1) {
        printf("my_rank: %d\n",my_rank);
        
        if (dm_h5_read_adi_info(h5_file_id,&nx,&ny,&nz,&error_is_present,
                                &my_adi_struct,
                                error_string,my_rank) == DM_FILEIO_FAILURE) {
            printf("Error reading \"/adi\" group info\n");
            dm_h5_close(h5_file_id,my_rank);
            exit(1);
	}
      
      if (error_is_present == 1) {
	printf("File has a group \"/adi\" with [%d,%d,%d] pixels, and error array\n",
	       nx,ny,nz);
      } else {
	printf("File has a group \"/adi\" with [%d,%d,%d] pixels, no error array\n",
	       nx,ny,nz);
      }
      
      if (adi_array_allocated == 1) 
	free(my_adi_array_struct.real_array);
      my_adi_array_struct.nx = nx;
      my_adi_array_struct.ny = ny;
      my_adi_array_struct.nz = nz;
      my_adi_array_struct.npix = (dm_array_index_t)nx*
	(dm_array_index_t)ny*(dm_array_index_t)nz;
      DM_ARRAY_REAL_STRUCT_INIT((&my_adi_array_struct),
                                my_adi_array_struct.npix,p);

      adi_array_allocated = 1;
      
      if (error_is_present == 1) {
	if (adi_error_array_allocated == 1) 
	  free(my_adi_error_array_struct.real_array);
	my_adi_error_array_struct.nx = my_adi_array_struct.nx;
	my_adi_error_array_struct.ny = my_adi_array_struct.ny;
	my_adi_error_array_struct.nz = my_adi_array_struct.nz;
	my_adi_error_array_struct.npix = my_adi_array_struct.npix;
        DM_ARRAY_REAL_STRUCT_INIT((&my_adi_error_array_struct),
                                  my_adi_error_array_struct.npix,p);

	adi_error_array_allocated = 1;
      } else {
	my_adi_error_array_struct.nx = 0;
	my_adi_error_array_struct.ny = 0;
	my_adi_error_array_struct.nz = 0;
	my_adi_error_array_struct.npix = 0;
      }
      
      /* We need this in case we use MPI */
      if (ny == 1) {
          xmax = my_adi_array_struct.nx/p;
          ymax = my_adi_array_struct.ny;
          zmax = my_adi_array_struct.nz;
      } else if (nz == 1) {
          xmax = my_adi_array_struct.nx;
          ymax = my_adi_array_struct.ny/p;
          zmax = my_adi_array_struct.nz;
      } else {
          xmax = my_adi_array_struct.nx;
          ymax = my_adi_array_struct.ny;
          zmax = my_adi_array_struct.nz/p;
      }
      
      if (dm_h5_read_adi(h5_file_id,&my_adi_array_struct,
			 &my_adi_error_array_struct,
			 error_string,my_rank,p) != DM_FILEIO_SUCCESS) {
	printf("%s\n", error_string);
	dm_h5_close(h5_file_id,my_rank);
	exit(1);
      }

      printf("adi_struct.lambda_meters = %.3le\n",
	     (double)my_adi_struct.lambda_meters);
      printf("adi_struct.xcenter_offset_pixels = %f\n",
	     (double)my_adi_struct.xcenter_offset_pixels);
      printf("adi_struct.error_scaling = %f\n",
	     (double)my_adi_struct.error_scaling);
      printf("First two rows of \"adi_array\":\n");
      iz = 0;
      for (iy=0; iy<print_limit; iy++) {
	for (ix=0; ix<print_limit; ix++) {
	  i = (ix+iy*my_adi_array_struct.nx+
	       iz*my_adi_array_struct.nx*my_adi_array_struct.ny);
	  printf("%.2f ",
		 (float)(*(my_adi_array_struct.real_array+i)));
	}
	printf("\n");
      }
      if (error_is_present == 1) {
	printf("First two rows of \"adi_error_array\":\n");
	iz = 0;
	for (iy=0; iy<print_limit; iy++) {
	  for (ix=0; ix<print_limit; ix++) {
	    i = (ix+iy*my_adi_error_array_struct.nx+
		 iz*my_adi_error_array_struct.nx*my_adi_error_array_struct.ny);
	    printf("%.2f ",
		   (float)(*(my_adi_error_array_struct.real_array+i)));
	  }
	  printf("\n");
	}
      }
    }
    
    if (dm_h5_ads_group_exists(h5_file_id,my_rank) == 1) {
      printf("File has a group \"/ads\"\n");
    }

    if (dm_h5_spt_group_exists(h5_file_id,my_rank) == 1) {
        printf("File has a group \"/spt\"\n");
        
        if (dm_h5_read_spt_info(h5_file_id,&nx,&ny,&nz,
                                &my_spt_struct,
                                error_string,my_rank) == DM_FILEIO_FAILURE) {
            printf("Error reading \"/spt\" group info\n");
            dm_h5_close(h5_file_id,my_rank);
            exit(1);
        }
	
        printf("spt_struct.support_scaling=%f\n",
               (double)my_spt_struct.support_scaling);
        
        if (spt_array_allocated == 1)
            free(my_spt_array_struct.byte_array);
        my_spt_array_struct.nx = nx;
        my_spt_array_struct.ny = ny;
        my_spt_array_struct.nz = nz;
        my_spt_array_struct.npix = 
            (dm_array_index_t)nx*(dm_array_index_t)ny*(dm_array_index_t)nz;
        DM_ARRAY_BYTE_STRUCT_INIT((&my_spt_array_struct),
                                  my_spt_array_struct.npix,p);
        
        spt_array_allocated = 1;

        /* We need this in case we use MPI */
      if (ny == 1) {
          xmax = my_spt_array_struct.nx/p;
          ymax = my_spt_array_struct.ny;
          zmax = my_spt_array_struct.nz;
      } else if (nz == 1) {
          xmax = my_spt_array_struct.nx;
          ymax = my_spt_array_struct.ny/p;
          zmax = my_spt_array_struct.nz;
      } else {
          xmax = my_spt_array_struct.nx;
          ymax = my_spt_array_struct.ny;
          zmax = my_spt_array_struct.nz/p;
      }

      if (dm_h5_read_spt(h5_file_id,&my_spt_array_struct,
			 error_string,my_rank,p) == DM_FILEIO_FAILURE) {
	printf("Error reading \"/spt\" array info\n");
	dm_h5_close(h5_file_id,my_rank);
	exit(1);
      }
      
      printf("First two rows of \"spt_array\":\n");
      iz = 0;
      for (iy=0; iy<print_limit; iy++) {
	for (ix=0; ix<print_limit; ix++) {
	  i = (ix+iy*my_spt_array_struct.nx+
	       iz*my_spt_array_struct.nx*my_spt_array_struct.ny);
	  printf("%d ",
		 (int)(*(my_spt_array_struct.byte_array+i)));
	}
	printf("\n");
      }
    } 

    if (dm_h5_itn_group_exists(h5_file_id,my_rank) == 1) {
      printf("File has a group \"/itn\"\n");

      if (dm_h5_read_itn_info(h5_file_id,&nx,&ny,&nz,
			      &recon_errors_npix, &my_itn_struct,
			      error_string,my_rank) == DM_FILEIO_FAILURE) {
	printf("Error reading \"/itn\" group info\n");
	dm_h5_close(h5_file_id,my_rank);
	exit(1);
      }

      printf("itn_struct.iterate_count=%d\n",
	     (int)my_itn_struct.iterate_count);

      if (my_rank == 0) {
          if (recon_errors_allocated == 1)
              free(recon_errors.real_array);
          
          if (recon_errors_npix > 0) {
              recon_errors.nx = recon_errors_npix;
              recon_errors.ny = 1;
              recon_errors.nz = 1;
              recon_errors.npix = recon_errors.nx;
              /* Recon errors will not be split up over the nodes 
               * so p = 1 in this case.
               */
              recon_errors.real_array = 
                  (dm_array_real *)malloc(recon_errors.npix*
                                          sizeof(dm_array_real));
              recon_errors_allocated = 1;
          }
      }

      if (itn_array_allocated == 1) 
	DM_ARRAY_COMPLEX_FREE(my_itn_array_struct.complex_array);
      my_itn_array_struct.nx = nx;
      my_itn_array_struct.ny = ny;
      my_itn_array_struct.nz = nz;
      my_itn_array_struct.npix = 
	(dm_array_index_t)nx*(dm_array_index_t)ny*(dm_array_index_t)nz;
      DM_ARRAY_COMPLEX_STRUCT_INIT((&my_itn_array_struct),
                                   my_itn_array_struct.npix,p);
      itn_array_allocated = 1;

      /* We need this in case we use MPI */
      if (ny == 1) {
          xmax = my_itn_array_struct.nx/p;
          ymax = my_itn_array_struct.ny;
          zmax = my_itn_array_struct.nz;
      } else if (nz == 1) {
          xmax = my_itn_array_struct.nx;
          ymax = my_itn_array_struct.ny/p;
          zmax = my_itn_array_struct.nz;
      } else {
          xmax = my_itn_array_struct.nx;
          ymax = my_itn_array_struct.ny;
          zmax = my_itn_array_struct.nz/p;
      }

      if (dm_h5_read_itn(h5_file_id,&recon_errors,&my_itn_array_struct,
			 error_string,my_rank,p) == DM_FILEIO_FAILURE) {
	printf("Error reading \"/itn\" array\n");
	dm_h5_close(h5_file_id,my_rank);
	exit(1);
      }

      if (my_rank == 0) {
          if (recon_errors.npix > 0) {
              printf("First %d elements of \"recon_errors\":\n",print_limit);
              for (ix=0;ix<print_limit;ix++) {
                  printf("%f ",*(recon_errors.real_array + ix));
              }
              printf("\n"); 
          } else {
              printf("No \"recon_errors\" found.\n");
          }
      }
      
      printf("First two [Re,Im] rows of \"itn_array\":\n");
      iz = 0;
      for (iy=0; iy<print_limit; iy++) {
	for (ix=0; ix<print_limit; ix++) {
	  i = (ix+iy*my_itn_array_struct.nx+
	       iz*my_itn_array_struct.nx*my_itn_array_struct.ny);
	  printf("[%.2f,%.2f] ",
		 (float)c_re(my_itn_array_struct.complex_array,i),
		 (float)c_im(my_itn_array_struct.complex_array,i));
	}
	printf("\n");
      }
      
    }
    dm_h5_close(h5_file_id,my_rank);

  } else if (is_readonly == 2) {
    /* in this case attempt to update and existing file */
    if (strlen(filename) == 0) {
      dm_test_fileio_help();
      exit(1);
    }
    
    if (dm_h5_openwrite(filename,&h5_file_id,error_string,my_rank) 
	!= DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      exit(1);
    }
    
    /* if (dm_h5_read_comments_info(h5_file_id,&n_strings,&string_length,
				 error_string) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
      } */
    my_comment_struct.n_strings_max = MAX_COMMENT_STRINGS;
    my_comment_struct.string_length = STRLEN;
   
    my_comment_struct.string_array = 
      (char *)malloc(my_comment_struct.n_strings_max*
		     my_comment_struct.string_length);
    my_comment_struct.specimen_name = 
      (char *)malloc(my_comment_struct.string_length);
    my_comment_struct.collection_date = 
      (char *)malloc(my_comment_struct.string_length);
    dm_clear_comments(&my_comment_struct);
    comstr_allocated = 1;

    dm_add_string_to_comments("In a world without",
			      &my_comment_struct);
    dm_add_string_to_comments("FENCES and WALLS, ",
			      &my_comment_struct);
    dm_add_string_to_comments("we don't need no WINDOWS or GATES!",
			      &my_comment_struct);

    if (dm_h5_add_comments(h5_file_id,&my_comment_struct,
			   error_string,my_rank) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }

    /* Get the existing adi_struct */
    if (dm_h5_read_adi_info(h5_file_id,&nx,&ny,&nz,&error_is_present,
			      &my_adi_struct,
                            error_string,my_rank) == DM_FILEIO_FAILURE) {
	printf("Error reading \"/adi\" group info\n");
	dm_h5_close(h5_file_id,my_rank);
	exit(1);
    }
       
    my_adi_array_struct.nx = 10;
    my_adi_array_struct.ny = 10;
    if (n_dims == 3) {
      my_adi_array_struct.nz = 10;
    } else {
      my_adi_array_struct.nz = 1;
    }
      
    my_adi_array_struct.npix = 
      (dm_array_index_t)my_adi_array_struct.nx*
      (dm_array_index_t)my_adi_array_struct.ny*
      (dm_array_index_t)my_adi_array_struct.nz;
    my_adi_array_struct.real_array = 
      (dm_array_real *)malloc(my_adi_array_struct.npix*
			      sizeof(dm_array_real));
    adi_array_allocated = 1;
    my_adi_struct.error_scaling = 2.;
    my_adi_struct.xcenter_offset_pixels = 4.0;

    /* We need this in case we use MPI */
    if (n_dims == 1) {
        xmax = my_adi_array_struct.nx/p;
        ymax = my_adi_array_struct.ny;
        zmax = my_adi_array_struct.nz;
    } else if (n_dims == 2) {
        xmax = my_adi_array_struct.nx;
        ymax = my_adi_array_struct.ny/p;
        zmax = my_adi_array_struct.nz;
    } else if (n_dims == 3){
        xmax = my_adi_array_struct.nx;
        ymax = my_adi_array_struct.ny;
        zmax = my_adi_array_struct.nz/p;
    }
    
    /* Get the existing spt_struct */
    if (dm_h5_read_spt_info(h5_file_id,&nx,&ny,&nz,
			      &my_spt_struct,
                            error_string,my_rank) == DM_FILEIO_FAILURE) {
	printf("Error reading \"/spt\" group info\n");
	dm_h5_close(h5_file_id,my_rank);
	exit(1);
    }

    my_spt_array_struct.nx = my_adi_array_struct.nx;
    my_spt_array_struct.ny = my_adi_array_struct.ny;
    my_spt_array_struct.nz = my_adi_array_struct.nz;
    my_spt_array_struct.npix = my_adi_array_struct.npix;
    my_spt_array_struct.byte_array = 
      (u_int8_t *)malloc(my_spt_array_struct.npix*
			 sizeof(u_int8_t));
    spt_array_allocated = 1;
    my_spt_struct.support_scaling = 3.;

    /* Get the existing itn_struct */
    if (dm_h5_read_itn_info(h5_file_id,&nx,&ny,&nz,
			    &recon_errors_npix,
			    &my_itn_struct,
			    error_string,my_rank) == DM_FILEIO_FAILURE) {
      printf("Error reading \"/itn\" group info\n");
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }    

    my_itn_array_struct.nx = my_adi_array_struct.nx;
    my_itn_array_struct.ny = my_adi_array_struct.ny;
    my_itn_array_struct.nz = my_adi_array_struct.nz;
    my_itn_array_struct.npix = my_adi_array_struct.npix;
    DM_ARRAY_COMPLEX_STRUCT_INIT((&my_itn_array_struct),
                                 my_itn_array_struct.npix,p);
    itn_array_allocated = 1;
    my_itn_struct.iterate_count = 20;

    if (my_rank == 0) {
        if (recon_errors_allocated == 1)
            free(recon_errors.real_array);
    
        recon_errors.nx = my_itn_struct.iterate_count;
        recon_errors.ny = 1;
        recon_errors.nz = 1;
        recon_errors.npix = recon_errors.nx;
        /* Recon errors will not be split up over the nodes 
         * so p = 1 in this case.
         */
        recon_errors.real_array = 
            (dm_array_real *)malloc(recon_errors.npix*sizeof(dm_array_real));
        recon_errors_allocated = 1;
        
        for (ix=0;ix<recon_errors.npix;ix++) {
            *(recon_errors.real_array + ix) = 
                (dm_array_real)1./(2.*(dm_array_real)(ix+1));
        }
    }

    /* These are the new arrays of different size than before */
    for (iz=0; iz<zmax; iz++) {
      for (iy=0; iy<ymax; iy++) {
	for (ix=0; ix<xmax; ix++) {
	  i = (ix+iy*my_adi_array_struct.nx+
	       iz*my_adi_array_struct.nx*my_adi_array_struct.ny);
	  *(my_adi_array_struct.real_array+i) = (dm_array_real)i;
	  *(my_spt_array_struct.byte_array+i) = (i % 256);
	  c_re(my_itn_array_struct.complex_array,i) = 
	    (dm_array_real)i;
	  c_im(my_itn_array_struct.complex_array,i) = 
	    (dm_array_real)0.5*i;
	}
      }
    }

    if (make_error == 1) {
      printf("Adding an error array\n");
      my_adi_error_array_struct.nx = my_adi_array_struct.nx;
      my_adi_error_array_struct.ny = my_adi_array_struct.ny;
      my_adi_error_array_struct.nz = my_adi_array_struct.nz;
      my_adi_error_array_struct.npix = my_adi_array_struct.npix;
      my_adi_error_array_struct.real_array = 
	(dm_array_real *)malloc(my_adi_error_array_struct.npix*
				sizeof(dm_array_real));
      adi_error_array_allocated = 1;
      
      for (iz=0; iz<zmax; iz++) {
	for (iy=0; iy<ymax; iy++) {
	  for (ix=0; ix<xmax; ix++) {
	    i = (ix+iy*my_adi_error_array_struct.nx+
		 iz*my_adi_error_array_struct.nx*my_adi_error_array_struct.ny);
	    *(my_adi_error_array_struct.real_array+i) = 1.;
	  }
	}
      }
    } else {
      my_adi_error_array_struct.nx = 0;
      my_adi_error_array_struct.ny = 0;
      my_adi_error_array_struct.nz = 0;
      my_adi_error_array_struct.npix = 0;
      my_adi_error_array_struct.real_array = NULL;
    }

    if (dm_h5_write_adi(h5_file_id,&my_adi_struct,
			&my_adi_array_struct,&my_adi_error_array_struct,
			error_string,my_rank,p) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }

    if (dm_h5_write_spt(h5_file_id,&my_spt_struct,
			&my_spt_array_struct,
			error_string,my_rank,p) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }

    if (dm_h5_write_itn(h5_file_id,&my_itn_struct,
			&my_itn_array_struct,&recon_errors,
			error_string,my_rank,p) != DM_FILEIO_SUCCESS) {
      printf("%s\n",error_string);
      dm_h5_close(h5_file_id,my_rank);
      exit(1);
    }
    printf("Updated file \"%s\"\n",filename);

    dm_h5_close(h5_file_id,my_rank);
  }

  if (comstr_allocated == 1) {
    free(my_comment_struct.string_array);
    free(my_comment_struct.specimen_name);
    free(my_comment_struct.collection_date);
  }
  if (adi_array_allocated == 1) 
    free(my_adi_array_struct.real_array);
  if (adi_error_array_allocated == 1)
    free(my_adi_error_array_struct.real_array);
  if (spt_array_allocated == 1)
    free(my_spt_array_struct.byte_array);
  if (itn_array_allocated == 1)
    DM_ARRAY_COMPLEX_FREE(my_itn_array_struct.complex_array);
  if (ainfo_allocated == 1) {
    free(my_ainfo_struct.file_directory);
    free(my_ainfo_struct.filename_array);
    free(my_ainfo_struct.systime_array);
    free(my_ainfo_struct.theta_x_radians_array);
    free(my_ainfo_struct.xcenter_offset_pixels_array);
    free(my_ainfo_struct.ycenter_offset_pixels_array);
  }
  if (my_rank == 0) {
      if (recon_errors_allocated == 1) {
          free(recon_errors.real_array);
      }
  }

  dm_exit();

  return(0);
}     

void dm_test_fileio_help() {
  
  printf("Usage: dm_test_fileio [-write -read filename -n x -error]\n");
  printf("  -read filename requires no further options.\n");
  printf("  -write makes its own filename.  Options:\n");
  printf("    -n x: make the array have x dimensions (e.g., \"-n 2\")\n");
  printf("    -error: add an error array to the ADI file.\n");
}
