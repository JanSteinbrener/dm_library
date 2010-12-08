/* This is the file dm_fileio.c */

#include "dm.h"
#include "dm_fileio.h"

/*--------------------------------------------------------------------*/
int dm_h5_create(char *filename, hid_t *ptr_h5_file_id,
                 char *error_string,int my_rank)
{
    /* Disable HDF's error reporting.  We'll be careful ourselves. */
    H5Eset_auto(NULL,NULL);

    if (my_rank == 0) {        
        /* Create a new file using H5F_ACC_TRUNC access,
         * default file creation properties, and default file
         * access properties.
         */
        if ((*ptr_h5_file_id = H5Fcreate(filename, H5F_ACC_TRUNC, 
                                         H5P_DEFAULT, H5P_DEFAULT)) < 0) {
            sprintf(error_string,"H5Fcreate(\"%s\") error",filename);
            H5Fclose(*ptr_h5_file_id);
            return(DM_FILEIO_FAILURE);
        }
    } /* endif(my_rank == 0) */
    return(DM_FILEIO_SUCCESS);
}

/*--------------------------------------------------------------------*/
int dm_h5_openwrite(char *filename, hid_t *ptr_h5_file_id,
                    char *error_string,
                     int my_rank)
{
    /* Disable HDF's error reporting.  We'll be careful ourselves. */
    H5Eset_auto(NULL,NULL);

    if (my_rank == 0) {
        
      if ((*ptr_h5_file_id =
	   H5Fopen(filename, H5F_ACC_RDWR, H5P_DEFAULT)) < 0) {
	sprintf(error_string,"H5Fopen(\"%s\") error\n",filename);
	return(DM_FILEIO_FAILURE);
      }
    } /* endif(my_rank == 0) */
    return(DM_FILEIO_SUCCESS);
}

/*--------------------------------------------------------------------*/
int dm_h5_openread(char *filename, hid_t *ptr_h5_file_id,
		   char *error_string,
                   int my_rank)
{
  /* Disable HDF's error reporting.  We'll be careful ourselves. */
    H5Eset_auto(NULL,NULL);
  
  if (my_rank == 0) {
    
    if ((*ptr_h5_file_id =
	 H5Fopen(filename, H5F_ACC_RDONLY, H5P_DEFAULT)) < 0) {
      sprintf(error_string,"H5Fopen(\"%s\") error\n",filename);
      return(DM_FILEIO_FAILURE);
    }
  } /* endif(my_rank == 0) */
  
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
void dm_h5_close(hid_t h5_file_id, int my_rank)
{
    if (my_rank == 0) {
        H5Fclose(h5_file_id);
    }
}

/*-------------------------------------------------------------------------*/
int dm_h5_create_comments(hid_t h5_file_id,
			  dm_comment_struct *ptr_comment_struct,
			  char *error_string,
                          int my_rank)
{
  hid_t int_datatype, int_dataspace;
  hid_t comments_group, cre_pid;
  hid_t datatype, dataspace, dataset;
  hsize_t int_dims[1], comstr_dims[1];
  hsize_t max_dims[1] = {H5S_UNLIMITED};
  hsize_t chunk_dims[1] = {ptr_comment_struct->n_strings};
  herr_t status; 
  hvl_t vl_comment_array[ptr_comment_struct->n_strings];
  int i, this_strlen;
  int offset = ptr_comment_struct->string_length;

  if (my_rank == 0) {
      /*--- We'll use the int datatype and dataspace several times ---*/
      int_dims[0] = 1;
      if ((int_datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(int_datatype) error");
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((int_dataspace = H5Screate_simple(1,int_dims,NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(int_dataspace) error");
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      
      /* Data will go into a group "/comments" in the file */
      if ((comments_group = H5Gcreate(h5_file_id,"/comments",0)) < 0) {
          strcpy(error_string,"H5Gcreate(\"/comments\") error");
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((dataset = H5Dcreate(comments_group,"comment_string_length",
			   int_datatype,int_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(comment_string_length) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,int_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             &ptr_comment_struct->string_length)) < 0) {
          strcpy(error_string,"H5Dwrite(comment_string_length) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      
      if ((dataset = H5Dcreate(comments_group,"n_comment_strings",
                               int_datatype,int_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(n_comment_strings) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,int_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             &ptr_comment_struct->n_strings)) < 0) {
          strcpy(error_string,"H5Dwrite(n_comment_strings) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      H5Sclose(int_dataspace);
      H5Tclose(int_datatype);
      
      if ((datatype = H5Tvlen_create(H5T_C_S1)) < 0) {
          strcpy(error_string,"H5Tvlen_create(comment_strings) error");
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      comstr_dims[0] = ptr_comment_struct->n_strings;
      if ((dataspace = H5Screate_simple(1,comstr_dims,max_dims)) < 0) {
          strcpy(error_string,"H5Screate_simple(comment_strings) error");
          H5Tclose(datatype);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((cre_pid = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
          strcpy(error_string,"H5Pcreate(comment_strings) error");
          H5Tclose(datatype);
          H5Sclose(dataspace);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Pset_chunk(cre_pid,1,chunk_dims)) < 0) {
          strcpy(error_string,"H5Pset_chunk(comment_strings) error");
          H5Tclose(datatype);
          H5Sclose(dataspace);
          H5Pclose(cre_pid);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((dataset = H5Dcreate(comments_group,"comment_strings",
                               datatype,dataspace,
                               cre_pid)) < 0) {
          strcpy(error_string,"H5Dcreate(comment_strings) error");
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Pclose(cre_pid);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      /* Now allocate and initialize the vl_comment_array */
      for (i=0;i<ptr_comment_struct->n_strings;i++) {
          this_strlen = strlen(&ptr_comment_struct->string_array[i*offset]);
          vl_comment_array[i].p = (char *)malloc(this_strlen*sizeof(char));
          vl_comment_array[i].len = this_strlen;
          strcpy((char *)vl_comment_array[i].p,
                 &ptr_comment_struct->string_array[i*offset]);
      }
      
      if ((status = H5Dwrite(dataset,datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             vl_comment_array)) < 0) {
          strcpy(error_string,"H5Dwrite(comment_strings) error");
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Pclose(cre_pid);
          H5Tclose(datatype);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = 
           H5Dvlen_reclaim(datatype,dataspace,
                           H5P_DEFAULT,vl_comment_array)) < 0) {
          strcpy(error_string,"H5Dvlen_reclaim(comment_strings) error");
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Pclose(cre_pid);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      H5Sclose(dataspace);
      H5Tclose(datatype);
      H5Pclose(cre_pid);
      
      if ((datatype = H5Tcopy(H5T_C_S1)) < 0) {
          strcpy(error_string,"H5Tcopy(specimen_name) error");
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      H5Tset_size(datatype,ptr_comment_struct->string_length);
      comstr_dims[0] = 1;
      if ((dataspace = H5Screate_simple(1,comstr_dims,NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(specimen_name) error");
          H5Tclose(datatype);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((dataset = H5Dcreate(comments_group,"specimen_name",
                               datatype,dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(specimen_name) error");
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Dwrite(dataset,datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_comment_struct->specimen_name)) < 0) {
          strcpy(error_string,"H5Awrite(specimen_name) error");
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      } 
      H5Dclose(dataset);
      
      if ((dataset = H5Dcreate(comments_group,"collection_date",
                               datatype,dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(collection_date) error");
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_comment_struct->collection_date)) < 0) {
          strcpy(error_string,"H5Awrite(collection_date) error");
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      } 
      H5Dclose(dataset);
      H5Sclose(dataspace);
      H5Tclose(datatype);
      H5Gclose(comments_group);
  } /* endif(my_rank == 0) */

  return(DM_FILEIO_SUCCESS);
}
/*-------------------------------------------------------------------------*/
int dm_h5_add_comments(hid_t h5_file_id,
		       dm_comment_struct *ptr_comment_struct,
		       char *error_string,
                       int my_rank)
{
  hid_t dataset, datatype, dataspace, memspace,xfer_pid;
  hid_t local_datatype;
  herr_t status;
  int string_length, n_strings, i, this_strlen;
  int new_strlen = ptr_comment_struct->string_length;
  hsize_t offset[1],newsize[1],comstr_dims[1];
  hvl_t vl_comment_array[ptr_comment_struct->n_strings];
  H5T_order_t local_order,file_order;

  if (my_rank == 0) {
      strcpy(error_string,"");
      
      /*--- determine local order, if we update make sure we convert
       * to file_order ---*/
      local_datatype = H5Tcopy(H5T_NATIVE_INT);
      if ((local_order = H5Tget_order(local_datatype)) < 0) {
          strcpy(error_string,"H5Tget_order(dm_h5_add_comments) error");
          return(DM_FILEIO_FAILURE);
      }
      H5Tclose(local_datatype);
      
      /* Get info of existing comments first */
      if (dm_h5_read_comments_info(h5_file_id,&n_strings,&string_length,
                                   error_string,my_rank) !=
          DM_FILEIO_SUCCESS) {
          return(DM_FILEIO_FAILURE);
      }
      offset[0] = n_strings;
      
      /* Update the stringlength if we need to */
      if (string_length < new_strlen) {
          if ((dataset = 
               H5Dopen(h5_file_id,"/comments/comment_string_length")) < 0) {
              strcpy(error_string,"H5Dopen(comment_string_length) error");
              return(DM_FILEIO_FAILURE);
          }
          
          if ((status = H5Dwrite(dataset,H5T_NATIVE_INT,
                                 H5S_ALL,H5S_ALL,H5P_DEFAULT,
                                 &ptr_comment_struct->string_length)) < 0) {
              strcpy(error_string,"H5Dwrite(comment_string_length) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              return(DM_FILEIO_FAILURE);
          }
          H5Dclose(dataset);
      } 
      
      /* Update the string counter */
      n_strings += ptr_comment_struct->n_strings;
      newsize[0] = n_strings;
      printf("new_strings: %d\n",n_strings);
      if ((dataset = 
           H5Dopen(h5_file_id,"/comments/n_comment_strings")) < 0) {
          strcpy(error_string,"H5Dopen(n_comment_strings) error");
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Dwrite(dataset,H5T_NATIVE_INT,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             &n_strings)) < 0) {
          strcpy(error_string,"H5Dwrite(n_comment_strings) error");
          H5Dclose(dataset);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      
      /* Now allocate and initialize the vl_comment_array */
      for (i=0;i<ptr_comment_struct->n_strings;i++) {
          this_strlen = strlen(&ptr_comment_struct->string_array[i*new_strlen]);
          vl_comment_array[i].p = (char *)malloc(this_strlen*sizeof(char));
          vl_comment_array[i].len = this_strlen;
          strcpy((char *)vl_comment_array[i].p,
                 &(ptr_comment_struct->string_array[i*new_strlen]));
      }
      
      if ((dataset = 
           H5Dopen(h5_file_id,"/comments/comment_strings")) < 0) {
          strcpy(error_string,"H5Dopen(comment_strings) error");
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Dextend(dataset,newsize)) < 0) {
          strcpy(error_string,"H5Dextend(comment_strings) error");
          H5Dclose(dataset);
          return(DM_FILEIO_FAILURE);
      }
      
      comstr_dims[0] = ptr_comment_struct->n_strings;
      if ((memspace = H5Screate_simple(1,comstr_dims,NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(comment_strings) error");
          H5Dclose(dataset);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Sget_space(comment_strings) error");
          H5Dclose(dataset);
          H5Sclose(memspace);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((datatype = H5Dget_type(dataset)) < 0) {
          strcpy(error_string,"H5Dget_type(comment_strings) error");
          H5Dclose(dataset);
          H5Sclose(memspace);
          H5Sclose(dataspace);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Sselect_hyperslab(dataspace,H5S_SELECT_SET,offset,NULL,
                                        comstr_dims,NULL)) < 0) {
          strcpy(error_string,"H5Sselect_hyperslab(comment_strings) error");
          H5Dclose(dataset);
          H5Sclose(memspace);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }

      if ((status = H5Dwrite(dataset,datatype,memspace,dataspace,H5P_DEFAULT,
                             vl_comment_array)) < 0) {
          strcpy(error_string,"H5Dwrite(comment_strings) error");
          H5Dclose(dataset);
          H5Sclose(memspace);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }
  
      /* free the memory from our vl structure */
      for (i=0;i<ptr_comment_struct->n_strings;i++) {
          free(vl_comment_array[i].p);
      }
      H5Dclose(dataset);
      H5Sclose(memspace);
      H5Sclose(dataspace);
      H5Tclose(datatype);
  } /* endif(my_rank == 0) */
  
  return(DM_FILEIO_SUCCESS);
}
/*-------------------------------------------------------------------------*/
int dm_h5_write_ainfo(hid_t h5_file_id,
		      dm_ainfo_struct *ptr_ainfo_struct,
		      char *error_string,
                      int my_rank)
{  
  hid_t int_datatype, int_dataspace;
  hid_t ainfo_group, attr;
  hid_t str_datatype, str_dataspace, strarr_dataspace, dataset;
  hid_t dbl_datatype, dblarr_dataspace;
  hsize_t int_dims[1], names_dims[1], double_dims[1];
  herr_t status;                             
  int dm_ainfo_version;

  if (my_rank == 0) {
      /* Check if n_frames_max is greater than n_frames */
      if (ptr_ainfo_struct->n_frames > ptr_ainfo_struct->n_frames_max) {
          strcpy(error_string,
                 "dm_h5_write_ainfo: n_frames greater than n_frames_max!");
          return(DM_FILEIO_FAILURE);
      }

      /* Then check array sizes of ainfo_struct. Add default values 
       * if necessary.
       */
      if (dm_check_ainfo(ptr_ainfo_struct, error_string,my_rank) !=
          DM_FILEIO_SUCCESS) {
          return(DM_FILEIO_FAILURE);
      }
  
      /*--- We'll use the int datatype and dataspace several times ---*/
      int_dims[0] = 1;
      if ((int_datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(int_datatype) error");
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }

      if ((int_dataspace = H5Screate_simple(1,int_dims,NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(int_dataspace) error");
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
  
      /* Data will go into a group "/ainfo" in the file */
      if ((ainfo_group = H5Gcreate(h5_file_id,"/ainfo",0)) < 0) {
          strcpy(error_string,"H5Gcreate(\"/ainfo\") error");
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
  
      if ((attr = H5Acreate(ainfo_group,"ainfo_version",
                            int_datatype,int_dataspace,
                            H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Acreate(ainfo_version) error");
          H5Aclose(attr);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      dm_ainfo_version = DM_AINFO_VERSION;
      if ((status = H5Awrite(attr,int_datatype,
                             &dm_ainfo_version)) < 0) {
          strcpy(error_string,"H5Awrite(ainfo_version) error");
          H5Aclose(attr);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Aclose(attr);

      if ((dataset = H5Dcreate(ainfo_group,"string_length",
                               int_datatype,int_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(string_length) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,int_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             &ptr_ainfo_struct->string_length)) < 0) {
          strcpy(error_string,"H5Dwrite(string_length) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
  
      if ((dataset = H5Dcreate(ainfo_group,"n_frames",
                               int_datatype,int_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(n_frames) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,int_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             &ptr_ainfo_struct->n_frames)) < 0) {
          strcpy(error_string,"H5Dwrite(n_frames) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);

      if ((dataset = H5Dcreate(ainfo_group,"no_background",
                               int_datatype,int_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(no_background) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,int_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             &ptr_ainfo_struct->no_background)) < 0) {
          strcpy(error_string,"H5Dwrite(no_background) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }

      H5Dclose(dataset);
      if ((dataset = H5Dcreate(ainfo_group,"dk_by_pix",
                               int_datatype,int_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(dk_by_pix) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,int_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             &ptr_ainfo_struct->dk_by_pix)) < 0) {
          strcpy(error_string,"H5Dwrite(dk_by_pix) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);

      if ((dataset = H5Dcreate(ainfo_group,"merge_first",
                               int_datatype,int_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(merge_first) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,int_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             &ptr_ainfo_struct->merge_first)) < 0) {
          strcpy(error_string,"H5Dwrite(merge_first) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          H5Sclose(int_dataspace);
          H5Tclose(int_datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      H5Sclose(int_dataspace);
      H5Tclose(int_datatype);

      if ((str_datatype = H5Tcopy(H5T_C_S1)) < 0) {
          strcpy(error_string,"H5Tcopy(file_directory) error");
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }

      H5Tset_size(str_datatype,ptr_ainfo_struct->string_length);

      names_dims[0] = 1;
      if ((str_dataspace = H5Screate_simple(1,names_dims,NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(file_directory) error");
          H5Sclose(str_dataspace);
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((dataset = H5Dcreate(ainfo_group,"file_directory",
                               str_datatype,str_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(file_directory) error");
          H5Dclose(dataset);
          H5Sclose(str_dataspace);
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,str_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_ainfo_struct->file_directory)) < 0) {
          strcpy(error_string,"H5Awrite(file_directory) error");
          H5Dclose(dataset);
          H5Sclose(str_dataspace);
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      } 
      H5Dclose(dataset);
      H5Sclose(str_dataspace);

      /* Start here with the string arrays (same datatype as file_directory)*/
      names_dims[0] = ptr_ainfo_struct->n_frames;
      if ((strarr_dataspace = H5Screate_simple(1,names_dims,NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(filename_array) error");
          H5Sclose(strarr_dataspace);
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }  

      if ((dataset = H5Dcreate(ainfo_group,"filename_array",
                               str_datatype,strarr_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(filename_array) error");
          H5Dclose(dataset);
          H5Sclose(strarr_dataspace);
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,str_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_ainfo_struct->filename_array)) < 0) {
          strcpy(error_string,"H5Dwrite(filename_array) error");
          H5Dclose(dataset);
          H5Sclose(strarr_dataspace);
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      } 
      H5Dclose(dataset);
  
      /* Same datatype and dataspace for the systime array */
      if ((dataset = H5Dcreate(ainfo_group,"systime_array",
                               str_datatype,strarr_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(systime_array) error");
          H5Dclose(dataset);
          H5Sclose(strarr_dataspace);
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,str_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_ainfo_struct->systime_array)) < 0) {
          strcpy(error_string,"H5Dwrite(systime_array) error");
          H5Dclose(dataset);
          H5Sclose(strarr_dataspace);
          H5Tclose(str_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      } 
      H5Dclose(dataset);
      H5Tclose(str_datatype);
      H5Sclose(strarr_dataspace);

      /* Start here with the double arrays (same dataspace as string arrays)*/
      if ((dbl_datatype = H5Tcopy(H5T_NATIVE_DOUBLE)) < 0) {
          strcpy(error_string,"H5Tcopy() error");
          H5Tclose(dbl_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }

      double_dims[0] = ptr_ainfo_struct->n_frames;
      if ((dblarr_dataspace = H5Screate_simple(1,double_dims,NULL)) < 0) {
          strcpy(error_string,
                 "H5Screate_simple() error");
          H5Sclose(dblarr_dataspace);
          H5Tclose(dbl_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }  

      if ((dataset = H5Dcreate(ainfo_group,"theta_x_radians_array",
                               dbl_datatype,dblarr_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(theta_x_radians_array) error");
          H5Dclose(dataset);
          H5Sclose(dblarr_dataspace);
          H5Tclose(dbl_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,dbl_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_ainfo_struct->theta_x_radians_array)) < 0) {
          strcpy(error_string,"H5Dwrite(theta_x_radians_array) error");
          H5Dclose(dataset);
          H5Sclose(dblarr_dataspace);
          H5Tclose(dbl_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      } 
      H5Dclose(dataset);

      if ((dataset = H5Dcreate(ainfo_group,"xcenter_offset_pixels_array",
                               dbl_datatype,dblarr_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(xcenter_offset_pixels_array) error");
          H5Dclose(dataset);
          H5Sclose(dblarr_dataspace);
          H5Tclose(dbl_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,dbl_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_ainfo_struct->xcenter_offset_pixels_array)) 
	  < 0) {
	strcpy(error_string,"H5Dwrite(xcenter_offset_pixels_array) error");
	H5Dclose(dataset);
	H5Sclose(dblarr_dataspace);
	H5Tclose(dbl_datatype);
	H5Gclose(ainfo_group);
	return(DM_FILEIO_FAILURE);
      } 
      H5Dclose(dataset);

      if ((dataset = H5Dcreate(ainfo_group,"ycenter_offset_pixels_array",
                               dbl_datatype,dblarr_dataspace,
                               H5P_DEFAULT)) < 0) {
          strcpy(error_string,"H5Dcreate(ycenter_offset_pixels_array) error");
          H5Dclose(dataset);
          H5Sclose(dblarr_dataspace);
          H5Tclose(dbl_datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dwrite(dataset,dbl_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_ainfo_struct->ycenter_offset_pixels_array)) 
	  < 0) {
	strcpy(error_string,"H5Dwrite(ycenter_offset_pixels_array) error");
	H5Dclose(dataset);
	H5Sclose(dblarr_dataspace);
	H5Tclose(dbl_datatype);
	H5Gclose(ainfo_group);
	return(DM_FILEIO_FAILURE);
      } 
      H5Dclose(dataset);

      H5Sclose(dblarr_dataspace);
      H5Tclose(dbl_datatype);
    
      H5Gclose(ainfo_group);
  } /* endif(my_rank == 0) */

  return(DM_FILEIO_SUCCESS);
}
/*-------------------------------------------------------------------------*/
int dm_h5_write_adi(hid_t h5_file_id,
		    dm_adi_struct *ptr_adi_struct,
		    dm_array_real_struct *ptr_adi_array_struct,
		    dm_array_real_struct *ptr_adi_error_array_struct,
		    char *error_string,
                    int my_rank,
                    int p)
{
  hid_t adi_group;
  hid_t datatype, dataspace, dataset,local_datatype;
  hid_t attr, cre_pid;
  hsize_t int_dims[1], adi_struct_dims[1];
  hsize_t *array_dims;
  hsize_t *array_maxdims;
  hsize_t *chunk_dims;
  herr_t status;                             
  int n_dims, dm_adi_version,i,exists;
  int nx, ny, nz, error_is_present;
  dm_adi_struct local_adi_struct;
  htri_t equal;
  H5T_order_t local_order, file_order;
#if USE_MPI
  MPI_Status mpi_status;
  hsize_t mem_dataspace;
  hsize_t *file_offsets,*file_counts;
  dm_array_real *slice_array;
#endif
  
  /*--- determine local order, if we update make sure we convert
   * to file_order ---*/
  
  local_datatype = H5Tcopy(H5T_NATIVE_INT);
  if ((local_order = H5Tget_order(local_datatype)) < 0) {
      strcpy(error_string,"H5Tget_order(dm_h5_write_adi) error");
      return(DM_FILEIO_FAILURE);
  }
  H5Tclose(local_datatype);
  
  strcpy(error_string,"");
  
  /* Check if file has an existing adi group.
   */
  exists = dm_h5_adi_group_exists(h5_file_id,my_rank);
          
  if (exists == 0) {
      if (my_rank == 0) {
          /* in this case we need to create the adi-group */
          if (ptr_adi_array_struct->ny == 1) {
              n_dims = 1;
          
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_adi_array_struct->nx;
          
              array_maxdims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_maxdims + 0) = H5S_UNLIMITED;
          
              chunk_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(chunk_dims + 0) = 
                  (ptr_adi_array_struct->nx/4 > 0) ?
                  ptr_adi_array_struct->nx/4 : 1;
          
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
          
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_adi_array_struct->nx/p;
#endif
          } else if (ptr_adi_array_struct->nz == 1) {
              n_dims = 2;
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_adi_array_struct->ny;
              *(array_dims + 1) = ptr_adi_array_struct->nx;
              array_maxdims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_maxdims + 0) = H5S_UNLIMITED;
              *(array_maxdims + 1) = H5S_UNLIMITED;
              chunk_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(chunk_dims + 0) = 
                  (ptr_adi_array_struct->ny/4 > 0) ?
                  ptr_adi_array_struct->ny/4 : 1;
              
              *(chunk_dims + 1) = 
                  (ptr_adi_array_struct->nx/4 > 0) ?
                  ptr_adi_array_struct->nx/4 : 1;
              
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0;
              *(file_offsets + 1) = 0; /* Will be adjusted for each slice */
          
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_adi_array_struct->ny/p;
              *(file_counts + 1) = ptr_adi_array_struct->nx;
#endif
          } else {
              n_dims = 3;
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_adi_array_struct->nz;
              *(array_dims + 1) = ptr_adi_array_struct->ny;
              *(array_dims + 2) = ptr_adi_array_struct->nx;
              array_maxdims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_maxdims + 0) = H5S_UNLIMITED;
              *(array_maxdims + 1) = H5S_UNLIMITED;
              *(array_maxdims + 2) = H5S_UNLIMITED;
              chunk_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(chunk_dims + 0) = 
                  (ptr_adi_array_struct->nz/4 > 0) ?
                  ptr_adi_array_struct->nz/4 : 1;
              *(chunk_dims + 1) = 
                  (ptr_adi_array_struct->ny/4 > 0) ?
                  ptr_adi_array_struct->ny/4 : 1;
              *(chunk_dims + 2) = 
                  (ptr_adi_array_struct->nx/4 > 0) ?
                  ptr_adi_array_struct->nx/4 : 1;
          
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0;
              *(file_offsets + 1) = 0;
              *(file_offsets + 2) = 0; /* Will be adjusted for each slice */
          
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_adi_array_struct->nz/p;
              *(file_counts + 1) = ptr_adi_array_struct->ny;
              *(file_counts + 2) = ptr_adi_array_struct->nx;
#endif
          }
      
          /* Data will go into a group "/adi" in the file */
          if ((adi_group = H5Gcreate(h5_file_id,"/adi",0)) < 0) {
              strcpy(error_string,"H5Gcreate(\"/adi\") error");
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
          int_dims[0] = 1;
          if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
              strcpy(error_string,"H5Tcopy(datatype) error");
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
          if ((dataspace = H5Screate_simple(1,int_dims,NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(dataspace) error");
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          if ((attr = H5Acreate(adi_group,"adi_version",
                                datatype,dataspace,
                                H5P_DEFAULT)) < 0) {
              strcpy(error_string,"H5Acreate(adi_version) error");
              H5Aclose(attr);
              H5Gclose(adi_group);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          dm_adi_version = DM_ADI_VERSION;
          if ((status = H5Awrite(attr,datatype,
                                 &dm_adi_version)) < 0) {
              strcpy(error_string,"H5Awrite(adi_version) error");
              H5Aclose(attr);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          H5Aclose(attr);
          H5Sclose(dataspace);
          H5Tclose(datatype);
      
          /*-----------------------------------------------------------------*/
          /* Now write the info in adi_struct into the "/adi" part of the file
           */
      
          adi_struct_dims[0] = 1;
          if ((datatype = H5Tcreate(H5T_COMPOUND,
                                    sizeof(dm_adi_struct))) < 0) {
              strcpy(error_string,"H5Tcreate(adi_struct) error");
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
          dm_h5_insert_adi_struct_members(datatype);
          if ((dataspace = H5Screate_simple(1,adi_struct_dims,NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(adi_struct)");
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
          if ((dataset = H5Dcreate(adi_group,"adi_struct",
                                   datatype,dataspace, 
                                   H5P_DEFAULT)) < 0) {
              strcpy(error_string,"H5Dcreate(adi_struct)");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
          if ((status = H5Dwrite(dataset,datatype, 
                                 H5S_ALL,H5S_ALL,H5P_DEFAULT,
                                 ptr_adi_struct)) < 0) {
              strcpy(error_string,"H5Dwrite(adi_struct)");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
      
#ifdef DM_ARRAY_DOUBLE
          datatype = H5Tcopy(H5T_NATIVE_DOUBLE);
#else
          datatype = H5Tcopy(H5T_NATIVE_FLOAT);
#endif
          if (datatype < 0) {
              strcpy(error_string,"H5Tcopy(adi_array) error");
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
          if ((dataspace =
               H5Screate_simple(n_dims, array_dims, array_maxdims)) < 0) {
              strcpy(error_string,"H5Screate_simple(adi_array) error");
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
#if USE_MPI
          if ((mem_dataspace =
               H5Screate_simple(n_dims, file_counts, NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(adi__array,memory) error");
              H5Sclose(dataspace);
              H5Gclose(adi_group);
              H5Tclose(datatype);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
              free(file_offsets);
              free(file_counts);
              return(DM_FILEIO_FAILURE);
          }
#endif
      
          if ((cre_pid = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
              strcpy(error_string,"H5Pcreate(adi_array) error");
              H5Tclose(datatype);
              H5Sclose(dataspace);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
          if ((status = H5Pset_chunk(cre_pid,n_dims,chunk_dims)) < 0) {
              strcpy(error_string,"H5Pset_chunk(adi_array) error");
              H5Pclose(cre_pid);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif      
              return(DM_FILEIO_FAILURE);
          }
      
          if ((dataset = H5Dcreate(adi_group,"adi_array",datatype, 
                                   dataspace, cre_pid)) < 0) {
              strcpy(error_string,"H5Dcreate(adi_array) error");
              H5Pclose(cre_pid);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      } /* endif(my_rank == 0) */
      
      /* 
       * Here we have to decide between MPI where the arrays are distributed
       * over the nodes and regular implementation
       */
#if USE_MPI
      /* Get the data from the nodes. npix will tell us the total number of
       * elements so we need to divide by the number of processes..
       */
      for (i = 0; i < p; i++) {

          if (i > 0) {
              if (my_rank == i) {
                  
                  MPI_Send(ptr_adi_array_struct->real_array,
                           ptr_adi_array_struct->npix/p, MPI_ARRAY_REAL, 0, 99,
                           MPI_COMM_WORLD);
                  
              } /* endif(my_rank == i) */
          } /* endif(i > 0) */
          
          if (my_rank == 0) {
              
              
              /* Determine the file_offsets here. Note that we have to divide by
               * number of processes.
               */
              if (n_dims == 1) {
                  file_offsets[0] = i*ptr_adi_array_struct->nx/p;
              } else if (n_dims == 2) {
                  file_offsets[0] = i*ptr_adi_array_struct->ny/p;
              } else if (n_dims == 3) {
                  file_offsets[0] = i*ptr_adi_array_struct->nz/p;
              }
              
              if ((status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                                file_offsets, NULL,
                                                file_counts, NULL)) < 0) {
                  strcpy(error_string,
                         "Error in H5Sselect_hyperslab(adi_array)");
                  H5Pclose(cre_pid);
                  H5Sclose(dataspace);
                  H5Sclose(mem_dataspace);
                  H5Tclose(datatype);
                  H5Gclose(adi_group);
                  free(array_dims);
                  free(array_maxdims);
                  free(chunk_dims);
                  free(file_offsets);
                  free(file_counts);
                  return(DM_FILEIO_FAILURE);
              }

              if (i == 0) {
                  /* Directly write adi_array_struct */
                  if ((status = H5Dwrite(dataset, datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                         ptr_adi_array_struct->real_array)) < 0) {
                      strcpy(error_string, "Error in H5Dwrite (adi_array)");
                      H5Pclose(cre_pid);
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Gclose(adi_group);
                      free(array_dims);
                      free(array_maxdims);
                      free(chunk_dims);
                      free(file_offsets);
                      free(file_counts);
                      return(DM_FILEIO_FAILURE);
                  }
              } else {
                  /* Receive slice to write */
                  slice_array =
                      (dm_array_real *)malloc(sizeof(dm_array_real)*
                                              ptr_adi_array_struct->npix/p);
                  
                  MPI_Recv(slice_array,ptr_adi_array_struct->npix/p,MPI_ARRAY_REAL,
                           i,99,MPI_COMM_WORLD, &mpi_status);
                  
                  if ((status = H5Dwrite(dataset, datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                         slice_array)) < 0) {
                      strcpy(error_string, "Error in H5Dwrite (adi_array)");
                      H5Pclose(cre_pid);
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Gclose(adi_group);
                      free(array_dims);
                      free(array_maxdims);
                      free(chunk_dims);
                      free(file_offsets);
                      free(file_counts);
                      free(slice_array);
                      return(DM_FILEIO_FAILURE);
                  }
                  
                  free(slice_array);
              } /* endif(i == 0) */
          }  /* endif(my_rank == 0) */ 
      } /* endfor */

      /* Now we can close the dataset */

      if (my_rank == 0) {
          H5Dclose(dataset);
      }
      
#else /* no USE_MPI */
       
      if ((status = H5Dwrite(dataset,datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_adi_array_struct->real_array)) < 0) {
          strcpy(error_string,"H5Dwrite(adi_array) error!");
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Gclose(adi_group);
          H5Pclose(cre_pid);
          free(array_dims);
          free(array_maxdims);
          free(chunk_dims);
          return(DM_FILEIO_FAILURE);
      }
      
      H5Dclose(dataset);
      
#endif /* USE_MPI */
      
      if (ptr_adi_error_array_struct->npix != 0) {
          if ((ptr_adi_error_array_struct->nx != ptr_adi_array_struct->nx) ||
              (ptr_adi_error_array_struct->ny != ptr_adi_array_struct->ny) ||
              (ptr_adi_error_array_struct->nz != ptr_adi_array_struct->nz) ||
              (ptr_adi_error_array_struct->npix != ptr_adi_array_struct->npix)) {
              sprintf(error_string,
                      "Bad error array: [%d,%d,%d] instead of [%d,%d,%d]",
                      (int)ptr_adi_error_array_struct->nx,
                      (int)ptr_adi_error_array_struct->ny,
                      (int)ptr_adi_error_array_struct->nz,
                      (int)ptr_adi_array_struct->nx,
                      (int)ptr_adi_array_struct->ny,
                      (int)ptr_adi_array_struct->nz);
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              H5Pclose(cre_pid);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif           
              return(DM_FILEIO_FAILURE);
          }

          if (my_rank == 0) {
              if ((dataset = H5Dcreate(adi_group,"adi_error_array",datatype, 
                                       dataspace, cre_pid)) < 0) {
                  strcpy(error_string,"H5Dcreate(adi_error_array) error");
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  H5Tclose(datatype);
                  H5Gclose(adi_group);
                  H5Pclose(cre_pid);
                  free(array_dims);
                  free(array_maxdims);
                  free(chunk_dims);
#if USE_MPI
                  free(file_offsets);
                  free(file_counts);
#endif
                  return(DM_FILEIO_FAILURE);
              }
          } /* endif(my_rank == 0) */

#if USE_MPI
          /* Get the data from the nodes. npix will tell us the total number of
           * elements so we need to divide by the number of processes..
           */
          for (i = 0; i < p; i++) {
              if (i > 0) {
                  if (my_rank == i) {
                      
                      MPI_Send(ptr_adi_error_array_struct->real_array,
                               ptr_adi_error_array_struct->npix/p,
                               MPI_ARRAY_REAL, 0, 99,MPI_COMM_WORLD);
                      
                  } /* endif(my_rank == i) */
              } /* endif(i > 0) */
              
              if (my_rank == 0) {
              
              
                  /* Determine the file_offsets here. Note that we have to
                   * divide by number of processes.
                   */
                  if (n_dims == 1) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->nx/p;
                  } else if (n_dims == 2) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->ny/p;
                  } else if (n_dims == 3) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->nz/p;
                  }
              
                  if ((status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                                    file_offsets, NULL,
                                                    file_counts, NULL)) < 0) {
                      strcpy(error_string,
                             "Error in H5Sselect_hyperslab(adi_array)");
                      H5Pclose(cre_pid);
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Gclose(adi_group);
                      free(array_dims);
                      free(array_maxdims);
                      free(chunk_dims);
                      free(file_offsets);
                      free(file_counts);
                      return(DM_FILEIO_FAILURE);
                  }

                  if (i == 0) {
                      /* Directly write adi_error_struct */
                      if ((status =
                           H5Dwrite(dataset, datatype, mem_dataspace,
                                    dataspace,H5P_DEFAULT,
                                    ptr_adi_error_array_struct->real_array)) < 0) {
                          strcpy(error_string, "Error in H5Dwrite (adi_array)");
                          H5Pclose(cre_pid);
                          H5Sclose(dataspace);
                          H5Sclose(mem_dataspace);
                          H5Tclose(datatype);
                          H5Gclose(adi_group);
                          free(array_dims);
                          free(array_maxdims);
                          free(chunk_dims);
                          free(file_offsets);
                          free(file_counts);
                          return(DM_FILEIO_FAILURE);
                      }
                  } else {
                      /* Receive slice to write */
                      slice_array =
                          (dm_array_real *)malloc(sizeof(dm_array_real)*
                                                  ptr_adi_error_array_struct->npix/p);
                      
                      MPI_Recv(slice_array,ptr_adi_error_array_struct->npix/p,
                               MPI_ARRAY_REAL,i,99,MPI_COMM_WORLD, &mpi_status);

                      if ((status = H5Dwrite(dataset, datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                             slice_array)) < 0) {
                          strcpy(error_string, "Error in H5Dwrite (adi_array)");
                          H5Pclose(cre_pid);
                          H5Sclose(dataspace);
                          H5Sclose(mem_dataspace);
                          H5Tclose(datatype);
                          H5Gclose(adi_group);
                          free(array_dims);
                          free(array_maxdims);
                          free(chunk_dims);
                          free(file_offsets);
                          free(file_counts);
                          free(slice_array);
                          return(DM_FILEIO_FAILURE);
                      }
                      
                      free(slice_array);
                  }  /* endif(my_rank == 0) */
              } /* endif(i == 0) */
          } /* endfor */
          
#else /* no USE_MPI */

          if ((status =
               H5Dwrite(dataset,datatype,
                        H5S_ALL,H5S_ALL,H5P_DEFAULT,
                        ptr_adi_error_array_struct->real_array)) < 0) {
              strcpy(error_string,"H5Dwrite(adi_error_array) error");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(adi_group);
              H5Pclose(cre_pid);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
              return(DM_FILEIO_FAILURE);
          }
          
#endif /* USE_MPI */   
      
      } /* endif (ptr_adi_error_array_struct->npix != 0) */
      
      if (my_rank == 0) {
          
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Gclose(adi_group);
          H5Pclose(cre_pid);
      
          /* Don't forget to free this */
          free(array_dims);
          free(array_maxdims);
          free(chunk_dims);
#if USE_MPI
          H5Sclose(mem_dataspace);
          free(file_offsets);
          free(file_counts);
#endif
      } /* endif(my_rank == 0) */
      
      return(DM_FILEIO_SUCCESS);
      
  } else if (exists == 1) {

      if (my_rank == 0) {
          
          /* first update the adi_struct */
          if ((dataset = H5Dopen(h5_file_id,"/adi/adi_struct")) < 0) {
              strcpy(error_string,"H5Dopen(adi_struct)");
              return(DM_FILEIO_FAILURE);
          }
          
          if ((datatype = H5Tcreate(H5T_COMPOUND,
                                    sizeof(dm_adi_struct))) < 0) {
              strcpy(error_string,"H5Tcreate(adi_struct) error");
              H5Tclose(datatype);
              H5Dclose(dataset);
              return(DM_FILEIO_FAILURE);
          }
          dm_h5_insert_adi_struct_members(datatype);
          
          if ((status = H5Dwrite(dataset,datatype, 
                                 H5S_ALL,H5S_ALL,H5P_DEFAULT,
                                 ptr_adi_struct)) < 0) {
              strcpy(error_string,"H5Dwrite(adi_struct)");
              H5Dclose(dataset);
              H5Tclose(datatype);
              return(DM_FILEIO_FAILURE);
          }
          H5Dclose(dataset);
          H5Tclose(datatype);
          
          /* in this case we just want to update an existing adi array */
          if (dm_h5_read_adi_info(h5_file_id,&nx,&ny,&nz,&error_is_present,
                                  &local_adi_struct,
                                  error_string,my_rank) == DM_FILEIO_FAILURE) {
              return(DM_FILEIO_FAILURE);
          }
      
          if ((dataset = H5Dopen(h5_file_id,"/adi/adi_array")) < 0) {
              strcpy(error_string,"H5Dopen(adi_array) error");
              return(DM_FILEIO_FAILURE);
          }
      
          if ((dataspace = H5Dget_space(dataset)) < 0) {
              strcpy(error_string,"H5Dget_space(adi_array) error");
              H5Dclose(dataset);
              return(DM_FILEIO_FAILURE);
          }
      
          if ((n_dims = H5Sget_simple_extent_ndims(dataspace)) < 0) {
              strcpy(error_string,"H5Sget_simple_extent_ndims(adi_array) error");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              return(DM_FILEIO_FAILURE);
          }
      
          /* First check if the new array is of the same dimensionality, and 
           * initialize the array_dims if so.
           */
          if (ptr_adi_array_struct->ny == 1) {
              if (n_dims != 1) {
                  sprintf(error_string,
                          "dm_h5_write_adi: New array must have dimensionality %d, not 1!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_adi_array_struct->nx;

#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_adi_array_struct->nx/p;
#endif
          } else if (ptr_adi_array_struct->nz == 1) {
              if (n_dims != 2) {
                  sprintf(error_string,
                          "dm_h5_write_adi: New array must have dimensionality %d, not 2!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_adi_array_struct->ny;
              *(array_dims + 1) = ptr_adi_array_struct->nx;
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
              *(file_offsets + 1) = 0;
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_adi_array_struct->ny/p;
              *(file_counts + 1) = ptr_adi_array_struct->nx;
#endif

          } else {
              if (n_dims != 3) {
                  sprintf(error_string,
                          "dm_h5_write_adi: New array must have dimensionality %d, not 3!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_adi_array_struct->nz;
              *(array_dims + 1) = ptr_adi_array_struct->ny;
              *(array_dims + 2) = ptr_adi_array_struct->nx;
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
              *(file_offsets + 1) = 0;
              *(file_offsets + 2) = 0;
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_adi_array_struct->nz/p;
              *(file_counts + 1) = ptr_adi_array_struct->ny;
              *(file_counts + 2) = ptr_adi_array_struct->nx;
#endif
          }
          
          /* Now see if the new array is of the same datatype as the existing 
           * array.
           */
          if ((datatype = H5Dget_type(dataset)) < 0) {
              strcpy(error_string, "H5Dget_type(adi_array) error");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              free(array_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          
          /*--- Get order of file ---*/
          if ((file_order = H5Tget_order(datatype)) < 0) {
              strcpy(error_string, "H5Tget_order(adi_array) error");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              free(array_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          
#ifdef DM_ARRAY_DOUBLE
          local_datatype = H5Tcopy(H5T_NATIVE_DOUBLE);
#else
          local_datatype = H5Tcopy(H5T_NATIVE_FLOAT);
#endif
          
          /*--- Convert to order if necessary so we can compare types ---*/
          if (local_order != file_order) {
              if ((status = H5Tset_order(local_datatype,file_order)) < 0) {
                  strcpy(error_string,"H5Tset_order(dm_h5_write_adi) error");
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  free(array_dims);
#if USE_MPI
                  free(file_offsets);
                  free(file_counts);
#endif
                  return(DM_FILEIO_FAILURE);
              } 
          } 
          
          if ((equal = H5Tequal(datatype,local_datatype)) < 1) {
              strcpy(error_string, 
                     "dm_h5_write_adi: Array of different type than existing array!");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              free(array_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      
          /*--- Convert to local_order if necessary for writing ---*/
          if (local_order != file_order) {
              if ((status = H5Tset_order(local_datatype,local_order)) < 0) {
                  strcpy(error_string,"H5Tset_order(dm_h5_write_adi) error");
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  free(array_dims);
#if USE_MPI
                  free(file_offsets);
                  free(file_counts);
#endif
                  return(DM_FILEIO_FAILURE);
              } 
          } 
      
          /* Now see if we have to extend the dataset. */
          if ((status = H5Dset_extent(dataset,array_dims)) < 0) {
              strcpy(error_string, "H5Dset_extent(adi_array) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Sclose(dataspace);
              H5Tclose(local_datatype);
              free(array_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      } /* endif(my_rank == 0) */

#if USE_MPI
      /* Create a memory dataspace */
      if ((mem_dataspace =
           H5Screate_simple(n_dims, file_counts, NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(adi__array,memory) error");
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Tclose(local_datatype);
          free(array_dims);
          free(file_offsets);
          free(file_counts);
          return(DM_FILEIO_FAILURE);
      }

      /* Get the data from the nodes. npix will tell us the total number of
       * elements so we need to divide by the number of processes..
       */
      for (i = 0; i < p; i++) {
          if (i > 0) {
              if (my_rank == i) {
                  
                  MPI_Send(ptr_adi_array_struct->real_array,
                           ptr_adi_array_struct->npix/p,
                           MPI_ARRAY_REAL, 0, 99,MPI_COMM_WORLD);
                  
              }
          } /* endif(i > 0) */
              
          if (my_rank == 0) {
              
              /* Determine the file_offsets here. Note that we have to
               * divide by number of processes.
               */
              if (n_dims == 1) {
                  file_offsets[0] = i*ptr_adi_array_struct->nx/p;
              } else if (n_dims == 2) {
                  file_offsets[0] = i*ptr_adi_array_struct->ny/p;
              } else if (n_dims == 3) {
                  file_offsets[0] = i*ptr_adi_array_struct->nz/p;
              }
              
              if ((status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                                file_offsets, NULL,
                                                file_counts, NULL)) < 0) {
                  strcpy(error_string,
                         "Error in H5Sselect_hyperslab(adi_array)");
                  H5Sclose(dataspace);
                  H5Sclose(mem_dataspace);
                  H5Tclose(datatype);
                  H5Dclose(dataset);
                  H5Tclose(local_datatype);
                  free(array_dims);
                  free(file_offsets);
                  free(file_counts);
                  return(DM_FILEIO_FAILURE);
              }

              if (i == 0) {
                  /* Directly write adi_array_struct */
                  if ((status = H5Dwrite(dataset, local_datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                         ptr_adi_array_struct->real_array)) < 0) {
                      strcpy(error_string, "Error in H5Dwrite (adi_array)!");
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Dclose(dataset);
                      H5Tclose(local_datatype);
                      free(array_dims);
                      free(file_offsets);
                      free(file_counts);
                      return(DM_FILEIO_FAILURE);
                  }
              } else {
                  /* Receive slice to write */
                  slice_array =
                      (dm_array_real *)malloc(sizeof(dm_array_real)*
                                              ptr_adi_array_struct->npix/p);
                  
                  MPI_Recv(slice_array,ptr_adi_array_struct->npix/p,
                           MPI_ARRAY_REAL,i,99,MPI_COMM_WORLD, &mpi_status);
                  
                  
                  if ((status = H5Dwrite(dataset, local_datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                         slice_array)) < 0) {
                      strcpy(error_string, "Error in H5Dwrite (adi_array)!");
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Dclose(dataset);
                      H5Tclose(local_datatype);
                      free(array_dims);
                      free(file_offsets);
                      free(file_counts);
                      free(slice_array);
                      return(DM_FILEIO_FAILURE);
                  }
                  
                  free(slice_array);
              }  /* endif(my_rank == 0) */
          } /* endif(i == 0) */
      } /* endfor */

#else /* no USE_MPI */
      /* Write the new array to the dataset. */
      if ((status = H5Dwrite(dataset,local_datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_adi_array_struct->real_array)) < 0) {
          strcpy(error_string,"H5Dwrite(adi_array) error.");
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Tclose(local_datatype);
          free(array_dims);
          return(DM_FILEIO_FAILURE);
      }
#endif /* USE_MPI */
      
      if (my_rank == 0) {
          H5Dclose(dataset);
          H5Tclose(datatype);
      }
      
      /* Do the same for the error array if applicable */
      if (ptr_adi_error_array_struct->npix != 0) {
          if ((ptr_adi_error_array_struct->nx != ptr_adi_array_struct->nx) ||
              (ptr_adi_error_array_struct->ny != ptr_adi_array_struct->ny) ||
              (ptr_adi_error_array_struct->nz != ptr_adi_array_struct->nz) ||
              (ptr_adi_error_array_struct->npix != ptr_adi_array_struct->npix)) {
              sprintf(error_string,
                      "Bad error array: [%d,%d,%d] instead of [%d,%d,%d]",
                      (int)ptr_adi_error_array_struct->nx,
                      (int)ptr_adi_error_array_struct->ny,
                      (int)ptr_adi_error_array_struct->nz,
                      (int)ptr_adi_array_struct->nx,
                      (int)ptr_adi_array_struct->ny,
                      (int)ptr_adi_array_struct->nz);
              H5Tclose(local_datatype);
              H5Sclose(dataspace);
              free(array_dims);
#if USE_MPI
              H5Sclose(mem_dataspace);
              free(file_counts);
              free(file_offsets);
#endif
              return(DM_FILEIO_FAILURE);
          }

          if (my_rank == 0) {
              if ((dataset = H5Dopen(h5_file_id,"/adi/adi_error_array")) < 0) {
                  strcpy(error_string,"H5Dopen(adi_error_array) error");
                  H5Tclose(local_datatype);
                  H5Sclose(dataspace);
                  free(array_dims);
#if USE_MPI
                  H5Sclose(mem_dataspace);
                  free(file_counts);
                  free(file_offsets);
#endif
                  return(DM_FILEIO_FAILURE);
              }
          
              /* Now see if the new array is of the same datatype as the
               * existing array.
               */
              if ((datatype = H5Dget_type(dataset)) < 0) {
                  strcpy(error_string, "H5Dget_type(adi_error_array) error");
                  H5Dclose(dataset);
                  H5Tclose(local_datatype);
                  H5Sclose(dataspace);
                  free(array_dims);
#if USE_MPI
                  H5Sclose(mem_dataspace);
                  free(file_counts);
                  free(file_offsets);
#endif
                  return(DM_FILEIO_FAILURE);
              }
          
              /*--- Convert to order if necessary so we can compare types ---*/
              if (local_order != file_order) {
                  if ((status = H5Tset_order(local_datatype,file_order)) < 0) {
                      strcpy(error_string,"H5Tset_order(dm_h5_write_adi) error");
                      H5Dclose(dataset);
                      H5Tclose(datatype);
                      H5Tclose(local_datatype);
                      H5Sclose(dataspace);
                      free(array_dims);
#if USE_MPI
                      H5Sclose(mem_dataspace);
                      free(file_counts);
                      free(file_offsets);
#endif
                      return(DM_FILEIO_FAILURE);
                  } 
              } 
          
              if ((equal = H5Tequal(datatype,local_datatype)) < 1) {
                  strcpy(error_string, 
                         "dm_h5_write_adi: Array of different type than existing array!");
                  H5Dclose(dataset);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  H5Sclose(dataspace);
                  free(array_dims);
#if USE_MPI
                  H5Sclose(mem_dataspace);
                  free(file_counts);
                  free(file_offsets);
#endif
                  return(DM_FILEIO_FAILURE);
              }
          
              /*--- Convert to local_order if necessary for writing ---*/
              if (local_order != file_order) {
                  if ((status = H5Tset_order(local_datatype,local_order)) < 0) {
                      strcpy(error_string,"H5Tset_order(dm_h5_write_adi) error");
                      H5Dclose(dataset);
                      H5Tclose(datatype);
                      H5Tclose(local_datatype);
                      H5Sclose(dataspace);
                      free(array_dims);
#if USE_MPI
                      H5Sclose(mem_dataspace);
                      free(file_counts);
                      free(file_offsets);
#endif
                      return(DM_FILEIO_FAILURE);
                  } 
              } 
          
              /* Now see if we have to extend the dataset. */
              if ((status = H5Dset_extent(dataset,array_dims)) < 0) {
                  strcpy(error_string, "H5Dset_extent(adi_error_array) error");
                  H5Dclose(dataset);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  H5Sclose(dataspace);
                  free(array_dims);
#if USE_MPI
                  H5Sclose(mem_dataspace);
                  free(file_counts);
                  free(file_offsets);
#endif
                  return(DM_FILEIO_FAILURE);
              }
          } /* endif(my_rank == 0) */

#if USE_MPI
          /* Get the data from the nodes. npix will tell us the total number of
           * elements so we need to divide by the number of processes..
           */
          for (i = 0; i < p; i++) {
              if (i > 0) {
                  if (my_rank == i) {
                      
                      MPI_Send(ptr_adi_error_array_struct->real_array,
                               ptr_adi_error_array_struct->npix/p,
                               MPI_ARRAY_REAL, 0, 99,MPI_COMM_WORLD);
                      
                  }
              } /* endif(i > 0) */
                  
              if (my_rank == 0) {
              
                  /* Determine the file_offsets here. Note that we have to
                   * divide by number of processes.
                   */
                  if (n_dims == 1) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->nx/p;
                  } else if (n_dims == 2) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->ny/p;
                  } else if (n_dims == 3) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->nz/p;
                  }
              
                  if ((status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                                    file_offsets, NULL,
                                                    file_counts, NULL)) < 0) {
                      strcpy(error_string,
                             "Error in H5Sselect_hyperslab(adi_array)");
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Sclose(dataspace);
                      free(array_dims);
                      free(file_offsets);
                      free(file_counts);
                      return(DM_FILEIO_FAILURE);
                  }
                  
                  if (i == 0) {
                      /* Directly write adi_error_array_struct */
                      if ((status =
                           H5Dwrite(dataset, local_datatype, mem_dataspace,
                                    dataspace,H5P_DEFAULT,
                                    ptr_adi_error_array_struct->real_array)) < 0) {
                          strcpy(error_string, "Error in H5Dwrite (adi_array)");
                          H5Sclose(dataspace);
                          H5Sclose(mem_dataspace);
                          H5Tclose(datatype);
                          H5Sclose(dataspace);
                          free(array_dims);
                          free(file_offsets);
                          free(file_counts);
                          return(DM_FILEIO_FAILURE);
                      }
                  } else {
                      /* Receive slice to write */
                      slice_array =
                          (dm_array_real *)malloc(sizeof(dm_array_real)*
                                                  ptr_adi_error_array_struct->npix/p);
                      
                      MPI_Recv(slice_array,ptr_adi_error_array_struct->npix/p,
                               MPI_ARRAY_REAL,i,99,MPI_COMM_WORLD, &mpi_status);
                      
                      
                      if ((status =
                           H5Dwrite(dataset, local_datatype, mem_dataspace,
                                    dataspace,H5P_DEFAULT,
                                    slice_array)) < 0) {
                          strcpy(error_string, "Error in H5Dwrite (adi_array)");
                          H5Sclose(dataspace);
                          H5Sclose(mem_dataspace);
                          H5Tclose(datatype);
                          H5Sclose(dataspace);
                          free(array_dims);
                          free(file_offsets);
                          free(file_counts);
                          free(slice_array);
                          return(DM_FILEIO_FAILURE);
                      }
                      
                      free(slice_array);
                  }  /* endif(my_rank == 0) */
              } /* endif(i == 0) */
          } /* endfor */
          

#else /* no USE_MPI */
          if ((status = H5Dwrite(dataset,local_datatype,
                                 H5S_ALL,H5S_ALL,H5P_DEFAULT,
                                 ptr_adi_error_array_struct->real_array)) < 0) {
              strcpy(error_string,"H5Dwrite(adi_error_array) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              H5Sclose(dataspace);
              free(array_dims);
              return(DM_FILEIO_FAILURE);
          }
#endif /* USE_MPI */
      } /* endif (ptr_adi_error_array_struct->npix != 0) */
      
      if (my_rank == 0) {
          H5Dclose(dataset);
          H5Tclose(datatype);
          H5Tclose(local_datatype);
          H5Sclose(dataspace);
          
          /* Don't forget to free this */
          free(array_dims);
#if USE_MPI
          H5Sclose(mem_dataspace);
          free(file_offsets);
          free(file_counts);
#endif
      }
      
      return(DM_FILEIO_SUCCESS);
      
  } /* endif(dm_h5_adi_group_exists(h5_file_id)) */
}

/*-------------------------------------------------------------------------*/
int dm_h5_write_spt(hid_t h5_file_id,
		    dm_spt_struct *ptr_spt_struct,
		    dm_array_byte_struct *ptr_spt_array_struct,
		    char *error_string,
                    int my_rank,
                    int p)
{
  hid_t spt_group;
  hid_t datatype, dataspace, dataset;
  hid_t attr, cre_pid,local_datatype;
  hsize_t int_dims[1], spt_struct_dims[1];
  hsize_t *array_dims;
  hsize_t *array_maxdims;
  hsize_t *chunk_dims;
  herr_t status;                             
  int n_dims, dm_spt_version,i,exists;
  int nx, ny, nz, nmembers;
  dm_spt_struct local_spt_struct;
  htri_t equal;
  H5T_order_t local_order, file_order;
#if USE_MPI
  MPI_Status mpi_status;
  hsize_t mem_dataspace;
  hsize_t *file_offsets,*file_counts;
  dm_array_real *slice_array;
#endif

  /*--- determine local order, if we update make sure we convert
   * to local_order ---*/
  local_datatype = H5Tcopy(H5T_NATIVE_INT);
  if ((local_order = H5Tget_order(local_datatype)) < 0) {
    strcpy(error_string,"H5Tget_order(dm_h5_write_spt) error");
    return(DM_FILEIO_FAILURE);
  }
  H5Tclose(local_datatype);

  strcpy(error_string,"");

  /* Check if file has an existing adi group.
   */
  exists = dm_h5_spt_group_exists(h5_file_id,my_rank);

  if (exists == 0) {
      if (my_rank == 0) {
          /* in this case we need to create the spt-group */
          if (ptr_spt_array_struct->ny == 1) {
              n_dims = 1;
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_spt_array_struct->nx;
              array_maxdims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_maxdims + 0) = H5S_UNLIMITED;
              chunk_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(chunk_dims + 0) = 
                  (ptr_spt_array_struct->nx/4 > 0) ?
                  ptr_spt_array_struct->nx/4 : 1;
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_spt_array_struct->nx/p;
#endif
          } else if (ptr_spt_array_struct->nz == 1) {
              n_dims = 2;
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_spt_array_struct->ny;
              *(array_dims + 1) = ptr_spt_array_struct->nx;
              array_maxdims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_maxdims + 0) = H5S_UNLIMITED;
              *(array_maxdims + 1) = H5S_UNLIMITED;
              chunk_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(chunk_dims + 0) = 
                  (ptr_spt_array_struct->ny/4 > 0) ?
                  ptr_spt_array_struct->ny/4 : 1;
              *(chunk_dims + 1) = 
                  (ptr_spt_array_struct->nx/4 > 0) ?
                  ptr_spt_array_struct->nx/4 : 1;
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0;
              *(file_offsets + 1) = 0; /* Will be adjusted for each slice */
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_spt_array_struct->ny/p;
              *(file_counts + 1) = ptr_spt_array_struct->nx;
#endif
          } else {
              n_dims = 3;
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_spt_array_struct->nz;
              *(array_dims + 1) = ptr_spt_array_struct->ny;
              *(array_dims + 2) = ptr_spt_array_struct->nx;
              array_maxdims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_maxdims + 0) = H5S_UNLIMITED;
              *(array_maxdims + 1) = H5S_UNLIMITED;
              *(array_maxdims + 2) = H5S_UNLIMITED;
              chunk_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(chunk_dims + 0) = 
                  (ptr_spt_array_struct->nz/4 > 0) ?
                  ptr_spt_array_struct->nz/4 : 1;
              *(chunk_dims + 1) = 
                  (ptr_spt_array_struct->ny/4 > 0) ?
                  ptr_spt_array_struct->ny/4 : 1;
              *(chunk_dims + 2) = 
                  (ptr_spt_array_struct->nx/4 > 0) ?
                  ptr_spt_array_struct->nx/4 : 1;
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0;
              *(file_offsets + 1) = 0;
              *(file_offsets + 2) = 0; /* Will be adjusted for each slice */
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_spt_array_struct->nz/p;
              *(file_counts + 1) = ptr_spt_array_struct->ny;
              *(file_counts + 2) = ptr_spt_array_struct->nx;
#endif
          }
    
          /* Data will go into a group "/spt" in the file */
          if ((spt_group = H5Gcreate(h5_file_id,"/spt",0)) < 0) {
              strcpy(error_string,"H5Gcreate(\"/spt\") error");
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
    
          int_dims[0] = 1;
          if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
              strcpy(error_string,"H5Tcopy(datatype) error");
              H5Tclose(datatype);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          if ((dataspace = H5Screate_simple(1,int_dims,NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(dataspace) error");
              H5Sclose(dataspace);
              H5Tclose(datatype);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          if ((attr = H5Acreate(spt_group,"spt_version",
                                datatype,dataspace,
                                H5P_DEFAULT)) < 0) {
              strcpy(error_string,"H5Acreate(spt_version) error");
              H5Aclose(attr);
              H5Gclose(spt_group);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          dm_spt_version = DM_SPT_VERSION;
          if ((status = H5Awrite(attr,datatype,
                                 &dm_spt_version)) < 0) {
              strcpy(error_string,"H5Awrite(spt_version) error");
              H5Aclose(attr);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          H5Aclose(attr);
          H5Sclose(dataspace);
          H5Tclose(datatype);
    
          /*-----------------------------------------------------------------*/
          /* Now write the info in spt_struct into the "/spt" part of the file */
    
          spt_struct_dims[0] = 1;
          if ((datatype = H5Tcreate(H5T_COMPOUND,
                                    sizeof(dm_spt_struct))) < 0) {
              strcpy(error_string,"H5Tcreate(spt_struct) error");
              H5Tclose(datatype);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          dm_h5_insert_spt_struct_members(datatype);
          if ((dataspace = H5Screate_simple(1,spt_struct_dims,NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(spt_struct)");
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
    
          if ((dataset = H5Dcreate(spt_group,"spt_struct",
                                   datatype,dataspace, 
                                   H5P_DEFAULT)) < 0) {
              strcpy(error_string,"H5Dcreate(spt_struct)");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
    
          if ((status = H5Dwrite(dataset,datatype, 
                                 H5S_ALL,H5S_ALL,H5P_DEFAULT,
                                 ptr_spt_struct)) < 0) {
              strcpy(error_string,"H5Dwrite(spt_struct)");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
    
          datatype = H5Tcopy(H5T_NATIVE_UINT8);
          if (datatype < 0) {
              strcpy(error_string,"H5Tcopy(spt_array) error");
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
    
          if ((dataspace =
               H5Screate_simple(n_dims, array_dims, array_maxdims)) < 0) {
              strcpy(error_string,"H5Screate_simple(spt_array) error");
              H5Tclose(datatype);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }

#if USE_MPI
          if ((mem_dataspace =
               H5Screate_simple(n_dims, file_counts, NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(adi__array,memory) error");
              H5Sclose(dataspace);
              H5Gclose(spt_group);
              H5Tclose(datatype);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
              free(file_offsets);
              free(file_counts);
              return(DM_FILEIO_FAILURE);
          }
#endif
    
          if ((cre_pid = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
              strcpy(error_string,"H5Pcreate(spt_array) error");
              H5Tclose(datatype);
              H5Sclose(dataspace);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
    
          if ((status = H5Pset_chunk(cre_pid,n_dims,chunk_dims)) < 0) {
              strcpy(error_string,"H5Pset_chunk(spt_array) error");
              H5Pclose(cre_pid);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
    
          if ((dataset = H5Dcreate(spt_group,"spt_array",datatype, 
                                   dataspace, cre_pid)) < 0) {
              strcpy(error_string,"H5Dcreate(spt_array) error");
              H5Pclose(cre_pid);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(spt_group);
              free(array_dims);
              free(array_maxdims);
              free(chunk_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      } /* endif(my_rank) == 0 */
      
      /* 
       * Here we have to decide between MPI where the arrays are distributed
       * over the nodes and regular implementation
       */
#if USE_MPI
      /* Get the data from the nodes. npix will tell us the total number of
       * elements so we need to divide by the number of processes..
       */
      for (i = 0; i < p; i++) {

          if (i > 0) {
              if (my_rank == i) {
                  
                  MPI_Send(ptr_spt_array_struct->byte_array,
                           ptr_spt_array_struct->npix/p, MPI_BYTE, 0, 99,
                           MPI_COMM_WORLD);
                  
              }
          } /* endif(i > 0) */
          
          if (my_rank == 0) {
              /* Determine the file_offsets here. Note that we have to divide by
               * number of processes.
               */
              if (n_dims == 1) {
                  file_offsets[0] = i*ptr_spt_array_struct->nx/p;
              } else if (n_dims == 2) {
                  file_offsets[0] = i*ptr_spt_array_struct->ny/p;
              } else if (n_dims == 3) {
                  file_offsets[0] = i*ptr_spt_array_struct->nz/p;
              }
              
              if ((status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                                file_offsets, NULL,
                                                file_counts, NULL)) < 0) {
                  strcpy(error_string,
                         "Error in H5Sselect_hyperslab(adi_array)");
                  H5Pclose(cre_pid);
                  H5Sclose(dataspace);
                  H5Sclose(mem_dataspace);
                  H5Tclose(datatype);
                  H5Gclose(spt_group);
                  free(array_dims);
                  free(array_maxdims);
                  free(chunk_dims);
                  free(file_offsets);
                  free(file_counts);
                  return(DM_FILEIO_FAILURE);
              }

              if (i == 0) {
                  /* Directly write spt_array_struct */
                  if ((status = H5Dwrite(dataset, datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                         ptr_spt_array_struct->byte_array)) < 0) {
                      strcpy(error_string, "Error in H5Dwrite (adi_array)");
                      H5Pclose(cre_pid);
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Gclose(spt_group);
                      free(array_dims);
                      free(array_maxdims);
                      free(chunk_dims);
                      free(file_offsets);
                      free(file_counts);
                      return(DM_FILEIO_FAILURE);
                  }
              } else {
                  /* Receive slice to write */
                  slice_array =
                      (dm_array_real *)malloc(sizeof(dm_array_real)*
                                              ptr_spt_array_struct->npix/p);
                  
                  MPI_Recv(slice_array,ptr_spt_array_struct->npix/p,MPI_BYTE,
                           i,99,MPI_COMM_WORLD, &mpi_status);

                  if ((status = H5Dwrite(dataset, datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                         slice_array)) < 0) {
                      strcpy(error_string, "Error in H5Dwrite (adi_array)");
                      H5Pclose(cre_pid);
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Gclose(spt_group);
                      free(array_dims);
                      free(array_maxdims);
                      free(chunk_dims);
                      free(file_offsets);
                      free(file_counts);
                      free(slice_array);
                      return(DM_FILEIO_FAILURE);
                  }
                  
                  free(slice_array);
              }  /* endif(my_rank == 0) */
          } /* endif(i == 0) */
      } /* endfor */

      
#else /* no USE_MPI */
      if ((status = H5Dwrite(dataset,datatype,
                             H5S_ALL,H5S_ALL,H5P_DEFAULT,
                             ptr_spt_array_struct->byte_array)) < 0) {
          strcpy(error_string,"H5Dwrite(spt_array) error");
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Gclose(spt_group);
          H5Pclose(cre_pid);
          free(array_dims);
          free(array_maxdims);
          free(chunk_dims);
          return(DM_FILEIO_FAILURE);
      } 

#endif /* USE_MPI */

      if (my_rank == 0) {
          
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Gclose(spt_group);
          H5Pclose(cre_pid);
          
          /* Don't forget to free this */
          free(array_dims);
          free(array_maxdims);
          free(chunk_dims);
#if USE_MPI
          H5Sclose(mem_dataspace);
          free(file_offsets);
          free(file_counts);
#endif
      }
    
      return(DM_FILEIO_SUCCESS);
      
  } else if (exists == 1) {

      if (my_rank == 0) {
          
          /* first update the spt_struct */
          if ((dataset = H5Dopen(h5_file_id,"/spt/spt_struct")) < 0) {
              strcpy(error_string,"H5Dopen(spt_struct)");
              return(DM_FILEIO_FAILURE);
          }

          if ((datatype = H5Tcreate(H5T_COMPOUND,
                                    sizeof(dm_spt_struct))) < 0) {
              strcpy(error_string,"H5Tcreate(spt_struct) error");
              H5Tclose(datatype);
              H5Dclose(dataset);
              return(DM_FILEIO_FAILURE);
          }
          dm_h5_insert_spt_struct_members(datatype);
    
          if ((status = H5Dwrite(dataset,datatype, 
                                 H5S_ALL,H5S_ALL,H5P_DEFAULT,
                                 ptr_spt_struct)) < 0) {
              strcpy(error_string,"H5Dwrite(spt_struct)");
              H5Dclose(dataset);
              H5Tclose(datatype);
              return(DM_FILEIO_FAILURE);
          }
          H5Dclose(dataset);
          H5Tclose(datatype);

          /* in this case we just want to update an existing spt array */
          if (dm_h5_read_spt_info(h5_file_id,&nx,&ny,&nz,
                                  &local_spt_struct,
                                  error_string,my_rank) == DM_FILEIO_FAILURE) {
              return(DM_FILEIO_FAILURE);
          }

          if ((dataset = H5Dopen(h5_file_id,"/spt/spt_array")) < 0) {
              strcpy(error_string,"H5Dopen(spt_array) error");
              return(DM_FILEIO_FAILURE);
          }
    
          if ((dataspace = H5Dget_space(dataset)) < 0) {
              strcpy(error_string,"H5Dget_space(spt_array) error");
              H5Dclose(dataset);
              return(DM_FILEIO_FAILURE);
          }

          if ((n_dims = H5Sget_simple_extent_ndims(dataspace)) < 0) {
              strcpy(error_string,"H5Sget_simple_extent_ndims(spt_array) error");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              return(DM_FILEIO_FAILURE);
          }

          /* First check if the new array is of the same dimensionality, and 
           * initialize the array_dims if so.
           */
          if (ptr_spt_array_struct->ny == 1) {
              if (n_dims != 1) {
                  sprintf(error_string,
                          "dm_h5_write_spt: New array must have dimensionality 1, not %d!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_spt_array_struct->nx;
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_spt_array_struct->nx/p;
#endif
          } else if (ptr_spt_array_struct->nz == 1) {
              if (n_dims != 2) {
                  sprintf(error_string,
                          "dm_h5_write_spt: New array must have dimensionality 2, not %d!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_spt_array_struct->ny;
              *(array_dims + 1) = ptr_spt_array_struct->nx;
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
              *(file_offsets + 1) = 0;
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_spt_array_struct->ny/p;
              *(file_counts + 1) = ptr_spt_array_struct->nx;
#endif
          } else {
              if (n_dims != 3) {
                  sprintf(error_string,
                          "dm_h5_write_spt: New array must have dimensionality 3, not %d!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              array_dims = (hsize_t *)malloc(n_dims*sizeof(hsize_t));
              *(array_dims + 0) = ptr_spt_array_struct->nz;
              *(array_dims + 1) = ptr_spt_array_struct->ny;
              *(array_dims + 2) = ptr_spt_array_struct->nx;
#if USE_MPI
              file_offsets = malloc(n_dims*sizeof(hsize_t));
              *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
              *(file_offsets + 1) = 0;
              *(file_offsets + 2) = 0;
              
              /* Note that we have to divide by the number of processes here.
               */
              file_counts = malloc(n_dims*sizeof(hsize_t));
              *(file_counts + 0) = ptr_spt_array_struct->nz/p;
              *(file_counts + 1) = ptr_spt_array_struct->ny;
              *(file_counts + 2) = ptr_spt_array_struct->nx;
#endif
          }

          /* Now get the datatype. */
          if ((datatype = H5Dget_type(dataset)) < 0) {
              strcpy(error_string, "H5Dget_type(spt_array) error");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              free(array_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }

          /*--- Get the order ---*/
          if ((file_order = H5Tget_order(datatype)) < 0) {
              strcpy(error_string, "H5Dget_order(spt_array) error");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              free(array_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }

          /*--- Convert to local_order if necessary for writing ---*/
          local_datatype = H5Tcopy(datatype);
          if (local_order != file_order) {
              if ((status = H5Tset_order(local_datatype,local_order)) < 0) {
                  strcpy(error_string,"H5Tset_order(dm_h5_write_spt) error");
                  H5Dclose(dataset);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  H5Sclose(dataspace);
                  free(array_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
                  return(DM_FILEIO_FAILURE);
              } 
          } 

          /* Now see if we have to extend the dataset. */
          if ((status = H5Dset_extent(dataset,array_dims)) < 0) {
              strcpy(error_string, "H5Dset_extent(spt_array) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Sclose(dataspace);
              H5Tclose(local_datatype);
              free(array_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
              return(DM_FILEIO_FAILURE);
          }
      } /* endif(my_rank == 0) */

#if USE_MPI
      /* Create a memory dataspace */
      if ((mem_dataspace =
           H5Screate_simple(n_dims, file_counts, NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(adi__array,memory) error");
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Tclose(local_datatype);
          free(array_dims);
          free(file_offsets);
          free(file_counts);
          return(DM_FILEIO_FAILURE);
      }

      /* Get the data from the nodes. npix will tell us the total number of
       * elements so we need to divide by the number of processes..
       */
      for (i = 0; i < p; i++) {
          if (i > 0) {
              if (my_rank == i) {
                  
                  MPI_Send(ptr_spt_array_struct->byte_array,
                           ptr_spt_array_struct->npix/p,
                           MPI_BYTE, 0, 99,MPI_COMM_WORLD);
                  
              }
          } /* endif(i > 0) */
              
          if (my_rank == 0) {
              
              /* Determine the file_offsets here. Note that we have to
               * divide by number of processes.
               */
              if (n_dims == 1) {
                  file_offsets[0] = i*ptr_spt_array_struct->nx/p;
              } else if (n_dims == 2) {
                  file_offsets[0] = i*ptr_spt_array_struct->ny/p;
              } else if (n_dims == 3) {
                  file_offsets[0] = i*ptr_spt_array_struct->nz/p;
              }
              
              if ((status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                                file_offsets, NULL,
                                                file_counts, NULL)) < 0) {
                  strcpy(error_string,
                         "Error in H5Sselect_hyperslab(adi_array)");
                  H5Sclose(dataspace);
                  H5Sclose(mem_dataspace);
                  H5Tclose(datatype);
                  H5Dclose(dataset);
                  H5Tclose(local_datatype);
                  free(array_dims);
                  free(file_offsets);
                  free(file_counts);
                  return(DM_FILEIO_FAILURE);
              }

              if (i == 0) {
                  if ((status = H5Dwrite(dataset, local_datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                         ptr_spt_array_struct->byte_array)) < 0) {
                      strcpy(error_string, "Error in H5Dwrite (adi_array)");
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Dclose(dataset);
                      H5Tclose(local_datatype);
                      free(array_dims);
                      free(file_offsets);
                      free(file_counts);
                      return(DM_FILEIO_FAILURE);
                  }
              } else {
                  slice_array =
                      (dm_array_real *)malloc(sizeof(dm_array_real)*
                                              ptr_spt_array_struct->npix/p);
                  
                  MPI_Recv(slice_array,ptr_spt_array_struct->npix/p,
                           MPI_BYTE,i,99,MPI_COMM_WORLD, &mpi_status);
                  
                  
                  if ((status = H5Dwrite(dataset, local_datatype, mem_dataspace,
                                         dataspace,H5P_DEFAULT,
                                         slice_array)) < 0) {
                      strcpy(error_string, "Error in H5Dwrite (adi_array)");
                      H5Sclose(dataspace);
                      H5Sclose(mem_dataspace);
                      H5Tclose(datatype);
                      H5Dclose(dataset);
                      H5Tclose(local_datatype);
                      free(array_dims);
                      free(file_offsets);
                      free(file_counts);
                      free(slice_array);
                      return(DM_FILEIO_FAILURE);
                  }
                  
                  free(slice_array);
              }  /* endif(my_rank == 0) */
          } /* endif(i == 0) */
      } /* endfor */

#else /* no USE_MPI */
      
    /* Write the new array to the dataset. */
    if ((status = H5Dwrite(dataset,local_datatype,
			   H5S_ALL,H5S_ALL,H5P_DEFAULT,
			   ptr_spt_array_struct->byte_array)) < 0) {
      strcpy(error_string,"H5Dwrite(spt_array) error");
      H5Dclose(dataset);
      H5Sclose(dataspace);
      H5Tclose(datatype);
      H5Tclose(local_datatype);
      free(array_dims);
      return(DM_FILEIO_FAILURE);
    }
#endif /* USE_MPI */

    if (my_rank == 0) {
        H5Dclose(dataset);
        H5Sclose(dataspace);
        H5Tclose(datatype);
        H5Tclose(local_datatype);
        
        /* Don't forget to free this */
        free(array_dims);
#if USE_MPI
        H5Sclose(mem_dataspace);
        free(file_offsets);
        free(file_counts);
#endif
    } /* endif(my_rank == 0) */
    return(DM_FILEIO_SUCCESS);

  } /* endif(dm_h5_spt_group_exists(h5_file_id) */
}

/*-------------------------------------------------------------------------*/
int dm_h5_write_itn(hid_t h5_file_id,
		    dm_itn_struct *ptr_itn_struct,
		    dm_array_complex_struct *ptr_itn_array_struct,
		    dm_array_real_struct *ptr_recon_errors,
		    char *error_string,
                    int my_rank,
                    int p)
{
  hid_t itn_group;
  hid_t datatype, dataspace, dataset, cre_pid, local_datatype;
  hid_t file_dataspace, memory_dataspace;
  hid_t attr;
  hsize_t int_dims[1], itn_struct_dims[1];
  hsize_t memory_dims[4], file_dims[4], file_offsets[4], file_counts[4];
  hsize_t array_maxdims[4],chunk_dims[4],recon_errors_chunkdim[1];
  hsize_t recon_errors_dim[1],recon_errors_maxdim[1] = {H5S_UNLIMITED};
  herr_t status;
  htri_t equal;
  int i,exists,nx,ny,nz,recon_errors_npix;
  int n_dims, dm_itn_version, n_complex;
  dm_array_index_t iz, zoffset, ipix, slice_npix;
  dm_array_real *slice_array;
  dm_itn_struct local_itn_struct;
  H5T_order_t order, local_order;
#if USE_MPI
  MPI_Status mpi_status;
  dm_array_real *temp_re,*temp_im;
#endif

  strcpy(error_string,"");

  /* Check if file has an existing itn group. */
  if ((exists = dm_h5_itn_group_exists(h5_file_id,my_rank)) == 0) {
      if (my_rank == 0) {
          /* Prepare for hyperslabs of the data.  "slice_npix" does not
           * reflect the factor of 2 for complex numbers; this is deliberate.
           */
          if (ptr_itn_array_struct->ny == 1) {
              n_dims = 2;
              file_dims[0] = ptr_itn_array_struct->nx;
              file_dims[1] = 2;
              file_offsets[0] = 0;
              file_offsets[1] = 0;
              
              array_maxdims[0] = H5S_UNLIMITED;
              array_maxdims[1] = file_dims[1];
              chunk_dims[0] = (file_dims[0]/4 > 0) ? file_dims[0]/4 : 1;
              chunk_dims[1] = file_dims[1];
#if USE_MPI
              memory_dims[0] = file_dims[0]/p;
              memory_dims[1] = file_dims[1];
              file_counts[0] = file_dims[0]/p;
              file_counts[1] = file_dims[1];
              slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
              memory_dims[0] = file_dims[0];
              memory_dims[1] = file_dims[1];
              file_counts[0] = memory_dims[0];
              file_counts[1] = memory_dims[1];
              slice_npix = ptr_itn_array_struct->npix;
#endif /* USE_MPI */
          } else if (ptr_itn_array_struct->nz == 1) {
              n_dims = 3;
              file_dims[0] = ptr_itn_array_struct->ny;
              file_dims[1] = ptr_itn_array_struct->nx;
              file_dims[2] = 2;
              file_offsets[0] = 0;
              file_offsets[1] = 0;
              file_offsets[2] = 0;
              array_maxdims[0] = H5S_UNLIMITED;
              array_maxdims[1] = H5S_UNLIMITED;
              array_maxdims[2] = file_dims[2];
              chunk_dims[0] = (file_dims[0]/4 > 0) ? file_dims[0]/4 : 1;
              chunk_dims[1] = (file_dims[1]/4 > 0) ? file_dims[1]/4 : 1;
              chunk_dims[2] = file_dims[2];
#if USE_MPI
              memory_dims[0] = file_dims[0]/p;
              memory_dims[1] = file_dims[1];
              memory_dims[2] = file_dims[2];
              file_counts[0] = file_dims[0]/p;
              file_counts[1] = file_dims[1];
              file_counts[2] = file_dims[2];
              slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
              memory_dims[0] = file_dims[0];
              memory_dims[1] = file_dims[1];
              memory_dims[2] = file_dims[2];
              file_counts[0] = memory_dims[0];
              file_counts[1] = memory_dims[1];
              file_counts[2] = memory_dims[2];
              slice_npix = ptr_itn_array_struct->npix;
#endif /* USE_MPI */
          } else {
              n_dims = 4;
              file_dims[0] = ptr_itn_array_struct->nz;
              file_dims[1] = ptr_itn_array_struct->ny;
              file_dims[2] = ptr_itn_array_struct->nx;
              file_dims[3] = 2;
              file_offsets[0] = 0; /* Will be adjusted for each slice */
              file_offsets[1] = 0;
              file_offsets[2] = 0;
              file_offsets[3] = 0;
              array_maxdims[0] = H5S_UNLIMITED;
              array_maxdims[1] = H5S_UNLIMITED;
              array_maxdims[2] = H5S_UNLIMITED;
              array_maxdims[3] = file_dims[3];
              chunk_dims[0] = (file_dims[0]/4 > 0) ? file_dims[0]/4 : 1;
              chunk_dims[1] = (file_dims[1]/4 > 0) ? file_dims[1]/4 : 1;
              chunk_dims[2] = (file_dims[2]/4 > 0) ? file_dims[2]/4 : 1;
              chunk_dims[3] = file_dims[3];
#if USE_MPI
              memory_dims[0] = file_dims[0]/p;
              memory_dims[1] = file_dims[1];
              memory_dims[2] = file_dims[2];
              memory_dims[3] = file_dims[3];

              file_counts[0] = file_dims[0]/p;
              file_counts[1] = file_dims[1];
              file_counts[2] = file_dims[2];
              file_counts[3] = file_dims[3];
              slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
              memory_dims[0] = 1;
              memory_dims[1] = file_dims[1];
              memory_dims[2] = file_dims[2];
              memory_dims[3] = file_dims[3];

              file_counts[0] = 1;
              file_counts[1] = memory_dims[1];
              file_counts[2] = memory_dims[2];
              file_counts[3] = memory_dims[3];
              slice_npix = (dm_array_index_t)(ptr_itn_array_struct->nx) *
                  (dm_array_index_t)(ptr_itn_array_struct->ny);
#endif /* USE_MPI */
          }
          
          /* Data will go into a group "/itn" in the file */
          if ((itn_group = H5Gcreate(h5_file_id,"/itn",0)) < 0) {
              strcpy(error_string,"H5Gcreate(\"/itn\") error");
              return(DM_FILEIO_FAILURE);
          }
    
          int_dims[0] = 1;
          if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
              strcpy(error_string,"H5Tcopy(datatype) error");
              H5Tclose(datatype);
              return(DM_FILEIO_FAILURE);
          }
          if ((dataspace = H5Screate_simple(1,int_dims,NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(dataspace) error");
              H5Sclose(dataspace);
              H5Tclose(datatype);
              return(DM_FILEIO_FAILURE);
          }
          if ((attr = H5Acreate(itn_group,"itn_version",
                                datatype,dataspace,
                                H5P_DEFAULT)) < 0) {
              strcpy(error_string,"H5Acreate(itn_version) error");
              H5Aclose(attr);
              H5Gclose(itn_group);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              return(DM_FILEIO_FAILURE);
          }
          dm_itn_version = DM_ITN_VERSION;
          if ((status = H5Awrite(attr,datatype,
                                 &dm_itn_version)) < 0) {
              strcpy(error_string,"H5Awrite(itn_version) error");
              H5Aclose(attr);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
          H5Aclose(attr);
          H5Sclose(dataspace);
          H5Tclose(datatype);
    
          /*-----------------------------------------------------------------*/
          /* Now write the info in itn_struct into the "/itn" part of
             the file */

          itn_struct_dims[0] = 1;
          if ((datatype = H5Tcreate(H5T_COMPOUND,
                                    sizeof(dm_itn_struct))) < 0) {
              strcpy(error_string,"H5Tcreate(itn_struct) error");
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
          dm_h5_insert_itn_struct_members(datatype);
		
          if ((dataspace = 
	       H5Screate_simple(1,itn_struct_dims,NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(itn_struct)");
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
    
          if ((dataset = H5Dcreate(itn_group,"itn_struct",
                                   datatype,dataspace, 
                                   H5P_DEFAULT)) < 0) {
              strcpy(error_string,"H5Dcreate(itn_struct)");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
    
          if ((status = H5Dwrite(dataset,datatype, 
                                 H5S_ALL,H5S_ALL,H5P_DEFAULT,
                                 ptr_itn_struct)) < 0) {
              strcpy(error_string,"H5Dwrite(itn_struct)");
              H5Dclose(dataset);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
          H5Dclose(dataset);
          H5Sclose(dataspace);
          H5Tclose(datatype);
    
	  /* Prepare to write recon_errors and itn_array */
#ifdef DM_ARRAY_DOUBLE
          datatype = H5Tcopy(H5T_NATIVE_DOUBLE);
#else
          datatype = H5Tcopy(H5T_NATIVE_FLOAT);
#endif

          if (datatype < 0) {
              strcpy(error_string,"H5Tcopy(itn_array) error");
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
	  
	  /* This is for recon_errors, only attempt to write it 
	   * if it is actually there
	   */

	  if (ptr_recon_errors->npix > 0) {
	    recon_errors_dim[0] = ptr_recon_errors->npix;
	    if ((dataspace = 
	        H5Screate_simple(1,recon_errors_dim,recon_errors_maxdim)) < 0) {
	      strcpy(error_string,"H5Screate_simple(recon_errors) error");
	      H5Tclose(datatype);
	      H5Gclose(itn_group);
	      return(DM_FILEIO_FAILURE);
	    }
	    
	    if ((cre_pid = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
              strcpy(error_string,"H5Pcreate(recon_errors) error");
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
	    }
	    
	    recon_errors_chunkdim[0] = ptr_recon_errors->npix/2;
	    if ((status = H5Pset_chunk(cre_pid,1,recon_errors_chunkdim)) < 0) {
              strcpy(error_string,"H5Pset_chunk(recon_errors) error");
              H5Pclose(cre_pid);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
	    }
	    
	    if ((dataset = H5Dcreate(itn_group,"recon_errors",datatype, 
				     dataspace, cre_pid)) < 0) {
              strcpy(error_string,"H5Dcreate(recon_errors) error");
              H5Pclose(cre_pid);
              H5Dclose(dataset);
              H5Sclose(dataspace);
	      H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
	    }
	    
	    if ((status = 
		 H5Dwrite(dataset, datatype, H5S_ALL, H5S_ALL,
			  H5P_DEFAULT,ptr_recon_errors->real_array)) < 0) {
	      strcpy(error_string, "Error in H5Dwrite (recon_errors)!");
	      H5Sclose(dataspace);
	      H5Tclose(datatype);
	      H5Gclose(itn_group);
	      H5Pclose(cre_pid);
	      H5Dclose(dataset);
	      return(DM_FILEIO_FAILURE);
	    }
	    
	    H5Pclose(cre_pid);
	    H5Dclose(dataset);
	    H5Sclose(dataspace);
	  } /* endif(ptr_recon_errors->npix > 0) */
	  
	  /* This is for itn_array */
          if ((file_dataspace = 
               H5Screate_simple(n_dims, file_dims, array_maxdims)) < 0) {
              strcpy(error_string,"H5Screate_simple(itn_array,file) error");
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
          if ((memory_dataspace =
               H5Screate_simple(n_dims, memory_dims, NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(itn_array,memory) error");
              H5Sclose(file_dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
    
          if ((cre_pid = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
              strcpy(error_string,"H5Pcreate(itn_array) error");
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
    
          if ((status = H5Pset_chunk(cre_pid,n_dims,chunk_dims)) < 0) {
              strcpy(error_string,"H5Pset_chunk(itn_array) error");
              H5Pclose(cre_pid);
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
    
          if ((dataset = H5Dcreate(itn_group,"itn_array",datatype, 
                                   file_dataspace, cre_pid)) < 0) {
              strcpy(error_string,"H5Dcreate(itn_array) error");
              H5Pclose(cre_pid);
              H5Dclose(dataset);
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              return(DM_FILEIO_FAILURE);
          }
          H5Pclose(cre_pid);
      } /* endif(my_rank == 0) */
    
      /* Now here is where we have to be careful for split versus
       * interleaved complex arrays.  We take care of this by
       * always making an interleaved array.  Remember that
       * we should not use sizeof(dm_array_complex) because it
       * might be an array of pointers.
       */
#if USE_MPI
      /* Get the data from the nodes. npix will tell us the total number of
       * elements so we need to divide by the number of processes..
       */
      for (i = 0; i < p; i++) {

          if (i > 0) {
              if (my_rank == i) {
#if DM_ARRAY_SPLIT
                  /* Need to send separate real and imaginary arrays */
                  MPI_Send((ptr_itn_array_struct->complex_array)->re,
                           ptr_itn_array_struct->npix/p, MPI_ARRAY_REAL, 0, 99,
                           MPI_COMM_WORLD);
                  MPI_Send((ptr_itn_array_struct->complex_array)->im,
                           ptr_itn_array_struct->npix/p, MPI_ARRAY_REAL, 0, 98,
                           MPI_COMM_WORLD);
#else
                  /* Just send twice as many elements for interleaved */
                  MPI_Send(ptr_itn_array_struct->complex_array,
                           2*ptr_itn_array_struct->npix/p, MPI_ARRAY_REAL, 0, 99,
                           MPI_COMM_WORLD);
#endif /* DM_ARRAY_SPLIT */
                  
              } /* endif(my_rank == i) */
          } /* endif(i > 0) */
              
          if (my_rank == 0) {
#if DM_ARRAY_SPLIT
              /* We will receive both im and re arrays and we need to map them
               * into an interleaved array
               */
              slice_array =
                  (dm_array_real *)malloc(2*sizeof(dm_array_real)*
                                          ptr_itn_array_struct->npix/p);

              if (i == 0) {
                  /* Directly map into one interleaved array */
                  for (ipix=0; ipix<ptr_itn_array_struct->npix/p; ipix++) {
                      *(slice_array+2*ipix) =
                          c_re(ptr_itn_array_struct->complex_array,ipix);
                      *(slice_array+2*ipix+1) =
                          c_im(ptr_itn_array_struct->complex_array,ipix);
                  } /* endfor */
              } else {
                  
                  temp_re = (dm_array_real *)malloc(sizeof(dm_array_real)*
                                                    ptr_itn_array_struct->npix/p);
                  temp_im = (dm_array_real *)malloc(sizeof(dm_array_real)*
                                                    ptr_itn_array_struct->npix/p);
                  
                  MPI_Recv(temp_re,ptr_itn_array_struct->npix/p,MPI_ARRAY_REAL,
                           i,99,MPI_COMM_WORLD, &mpi_status);
                  MPI_Recv(temp_im,ptr_itn_array_struct->npix/p,MPI_ARRAY_REAL,
                           i,98,MPI_COMM_WORLD, &mpi_status);
              

                  /* Now map into one interleaved array */
                  for (ipix=0; ipix<ptr_itn_array_struct->npix/p; ipix++) {
                      *(slice_array+2*ipix) = *(temp_re+ipix);
                      *(slice_array+2*ipix+1) =*(temp_im+ipix);
                  } /* endfor */
                  
                  free(temp_re);
                  free(temp_im);
              } /* endif(i == 0) */
#else
              /* We will receive an interleaved array */
              slice_array =
                  (dm_array_real *)malloc(2*sizeof(dm_array_real)*
                                          ptr_itn_array_struct->npix/p);
              
              if (i == 0) {
                  /* Directly map into one interleaved array */
                  for (ipix=0; ipix<ptr_itn_array_struct->npix/p; ipix++) {
                      *(slice_array+2*ipix) =
                          c_re(ptr_itn_array_struct->complex_array,ipix);
                      *(slice_array+2*ipix+1) =
                          c_im(ptr_itn_array_struct->complex_array,ipix);
                  } /* endfor */
              } else {
                  
                  MPI_Recv(slice_array,2*ptr_itn_array_struct->npix/p,
                           MPI_ARRAY_REAL,
                           i,99,MPI_COMM_WORLD, &mpi_status);
              } /* endif(i == 0) */
              
#endif /* DM_ARRAY_SPLIT */

              /* Determine the file_offsets here. Note that we have to divide by
               * number of processes. Note that we always have at least 2
               * dimensions because it is complex.
               */
              if (n_dims == 2) {
                  file_offsets[0] = i*ptr_itn_array_struct->nx/p;
              } else if (n_dims == 3) {
                  file_offsets[0] = i*ptr_itn_array_struct->ny/p;
              } else if (n_dims == 4) {
                  file_offsets[0] = i*ptr_itn_array_struct->nz/p;
              }
              
              if ((status = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,
                                                file_offsets, NULL,
                                                file_counts, NULL)) < 0) {
                  strcpy(error_string,
                         "Error in H5Sselect_hyperslab(itn_array)");
                  H5Sclose(file_dataspace);
                  H5Sclose(memory_dataspace);
                  H5Tclose(datatype);
                  H5Gclose(itn_group);
                  free(slice_array);
                  return(DM_FILEIO_FAILURE);
              }
              
              if ((status = H5Dwrite(dataset, datatype, memory_dataspace,
                                     file_dataspace,H5P_DEFAULT,
                                     slice_array)) < 0) {
                  strcpy(error_string, "Error in H5Dwrite (itn_array)!");
                  H5Sclose(file_dataspace);
                  H5Sclose(memory_dataspace);
                  H5Tclose(datatype);
                  H5Gclose(itn_group);
                  free(slice_array);
                  return(DM_FILEIO_FAILURE);
              }
              
              free(slice_array);
          }  /* endif(my_rank == 0) */ 
      } /* endfor */

#else /* no USE_MPI */
      slice_array = (dm_array_real *)malloc(2*sizeof(dm_array_real)*slice_npix);
      if (slice_array == NULL) {
          strcpy(error_string,"slice malloc() error");
          H5Dclose(dataset);
          H5Sclose(file_dataspace);
          H5Sclose(memory_dataspace);
          H5Tclose(datatype);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      for (iz=0; iz<(ptr_itn_array_struct->nz); iz++) {
          file_offsets[0] = iz;
          zoffset = iz*slice_npix;
          for (ipix=0; ipix<slice_npix; ipix++) {
              *(slice_array+2*ipix) = 
                  c_re(ptr_itn_array_struct->complex_array,(ipix+zoffset));
              *(slice_array+2*ipix+1) =
                  c_im(ptr_itn_array_struct->complex_array,(ipix+zoffset));
          }
      
          if ((status = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,
                                            file_offsets,NULL,
                                            file_counts,NULL)) < 0) {
              strcpy(error_string,"Error in H5Sselect_hyperslab(itn_array)");
              H5Dclose(dataset);
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              free(slice_array);
              return(DM_FILEIO_FAILURE);
          }
          if ((status = H5Dwrite(dataset,datatype,memory_dataspace,
                                 file_dataspace,H5P_DEFAULT,slice_array)) < 0) {
              strcpy(error_string,"Error in H5Dwrite(itn_array)");
              H5Dclose(dataset);
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              H5Gclose(itn_group);
              free(slice_array);
              return(DM_FILEIO_FAILURE);
          }
      }
      
      free(slice_array);
#endif /* USE_MPI */
      
      if (my_rank == 0) {
          H5Dclose(dataset);
          H5Sclose(file_dataspace);
          H5Sclose(memory_dataspace);
          H5Tclose(datatype);
          H5Gclose(itn_group);
      } /* endif(my_rank == 0) */
      
      return(DM_FILEIO_SUCCESS);
      
  } else if ((exists = dm_h5_itn_group_exists(h5_file_id,my_rank)) == 1) {

      if (my_rank == 0) {
	
	/* first update the itn_struct */
	if ((dataset = H5Dopen(h5_file_id,"/itn/itn_struct")) < 0) {
	  strcpy(error_string,"H5Dopen(itn_struct)");
	  return(DM_FILEIO_FAILURE);
	}
	
          if ((datatype = H5Tcreate(H5T_COMPOUND,
                                    sizeof(dm_itn_struct))) < 0) {
              strcpy(error_string,"H5Tcreate(itn_struct) error");
              H5Tclose(datatype);
              return(DM_FILEIO_FAILURE);
          }
          dm_h5_insert_itn_struct_members(datatype);
				
          if ((status = H5Dwrite(dataset,datatype, 
                                 H5S_ALL,H5S_ALL,H5P_DEFAULT,
                                 ptr_itn_struct)) < 0) {
              strcpy(error_string,"Error in H5Dwrite(itn_struct)");
              H5Dclose(dataset);
              H5Tclose(datatype);
              return(DM_FILEIO_FAILURE);
          }
          H5Dclose(dataset);
          H5Tclose(datatype);

	  /* Check on sizes of itn_array and recon_errors */
          if (dm_h5_read_itn_info(h5_file_id,&nx,&ny,&nz,
				  &recon_errors_npix,
                                  &local_itn_struct,
                                  error_string,
                                  my_rank) == DM_FILEIO_FAILURE) {
              return(DM_FILEIO_FAILURE);
          }

	  /* See if we have an existing recon_errors dataset */
	  if (recon_errors_npix == -1) {
	    /* We want to create the dataset unless we don't have any
	     * errors to put in it.
	     */
	    if (ptr_recon_errors->npix > 0) {
	      if ((itn_group = H5Gopen(h5_file_id,"/itn")) < 0) {
		strcpy(error_string,"H5Gopen(\"/itn\") error");
		return(DM_FILEIO_FAILURE);
	      }
	      
	      recon_errors_dim[0] = ptr_recon_errors->npix;
	      if ((dataspace = 
		   H5Screate_simple(1,recon_errors_dim,recon_errors_maxdim)) < 0) {
		strcpy(error_string,"H5Screate_simple(recon_errors) error");
		H5Tclose(datatype);
		H5Gclose(itn_group);
		return(DM_FILEIO_FAILURE);
	      }
	      
	      if ((cre_pid = H5Pcreate(H5P_DATASET_CREATE)) < 0) {
		strcpy(error_string,"H5Pcreate(recon_errors) error");
		H5Sclose(dataspace);
		H5Tclose(datatype);
		H5Gclose(itn_group);
		return(DM_FILEIO_FAILURE);
	      }
	      
	      recon_errors_chunkdim[0] = ptr_recon_errors->npix/2;
	      if ((status = H5Pset_chunk(cre_pid,1,recon_errors_chunkdim)) < 0) {
		strcpy(error_string,"H5Pset_chunk(recon_errors) error");
		H5Pclose(cre_pid);
		H5Sclose(dataspace);
		H5Tclose(datatype);
		H5Gclose(itn_group);
		return(DM_FILEIO_FAILURE);
	      }
	      
#ifdef DM_ARRAY_DOUBLE
	      datatype = H5Tcopy(H5T_NATIVE_DOUBLE);
#else
	      datatype = H5Tcopy(H5T_NATIVE_FLOAT);
#endif
	      
	      if ((dataset = H5Dcreate(itn_group,"recon_errors",datatype, 
				       dataspace, cre_pid)) < 0) {
		strcpy(error_string,"H5Dcreate(recon_errors) error");
		H5Pclose(cre_pid);
		H5Dclose(dataset);
		H5Sclose(dataspace);
		H5Tclose(datatype);
		H5Gclose(itn_group);
		return(DM_FILEIO_FAILURE);
	      }
	      
	      if ((status = 
		   H5Dwrite(dataset, datatype, H5S_ALL, H5S_ALL,
			    H5P_DEFAULT,ptr_recon_errors->real_array)) < 0) {
		strcpy(error_string, "Error in H5Dwrite (recon_errors)!");
		H5Sclose(dataspace);
		H5Tclose(datatype);
		H5Gclose(itn_group);
		H5Pclose(cre_pid);
		H5Dclose(dataset);
		return(DM_FILEIO_FAILURE);
	      }
	      
	      H5Pclose(cre_pid);
	      H5Dclose(dataset);
	      H5Sclose(dataspace);
	      H5Gclose(itn_group);
	      H5Tclose(datatype);
	    } /* endif(ptr_recon_errors->npix > 0) */
	  } else {
	    /* Update existing recon_errors */
	    if ((dataset = H5Dopen(h5_file_id,"/itn/recon_errors")) < 0) {
              strcpy(error_string,"H5Dopen(recon_errors) error");
              return(DM_FILEIO_FAILURE);
	    }
    
	    /* Now get the datatype. */
	    if ((datatype = H5Dget_type(dataset)) < 0) {
              strcpy(error_string, "H5Dget_type(recon_errors) error");
              H5Dclose(dataset);
              return(DM_FILEIO_FAILURE);
	    }
	    
	    /*--- get order of type ---*/
	    if ((order = H5Tget_order(datatype)) < 0) {
              strcpy(error_string,"H5Tget_order(recon_errors) error");
              H5Dclose(dataset);
              H5Sclose(dataspace);
	      return(DM_FILEIO_FAILURE);
	    }
	    
#ifdef DM_ARRAY_DOUBLE
	    local_datatype = H5Tcopy(H5T_NATIVE_DOUBLE);
#else
	    local_datatype = H5Tcopy(H5T_NATIVE_FLOAT);
#endif
	    
	    /*--- get order of local type ---*/
	    if ((local_order = H5Tget_order(local_datatype)) < 0) {
              strcpy(error_string,"H5Tget_order(recon_errors) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              return(DM_FILEIO_FAILURE);
	    }
    
	    /*--- Convert to order if necessary so we can compare types ---*/
	    if (local_order != order) {
              if ((status = H5Tset_order(local_datatype,order)) < 0) {
		strcpy(error_string,"H5Tset_order(recon_errors) error");
		H5Dclose(dataset);
		H5Tclose(datatype);
		H5Tclose(local_datatype);
		return(DM_FILEIO_FAILURE);
              } 
	    } 

	    if ((equal = H5Tequal(datatype,local_datatype)) < 1) {
              strcpy(error_string, 
                     "dm_h5_write_itn: recon_errors of different type than existing recon_errors!");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              return(DM_FILEIO_FAILURE);
	    }
	    
	    /*--- Convert back to local_order if necessary for writing ---*/
	    if (local_order != order) {
              if ((status = H5Tset_order(local_datatype,local_order)) < 0) {
		strcpy(error_string,"H5Tset_order(recon_errors) error");
		H5Dclose(dataset);
		H5Tclose(datatype);
		H5Tclose(local_datatype);
		return(DM_FILEIO_FAILURE);
              } 
	    } 
	    H5Tclose(datatype);
	    
	    /* Now see if we have to extend the dataset. */
	    recon_errors_dim[0] = ptr_recon_errors->npix;
	    if ((status = H5Dset_extent(dataset,recon_errors_dim)) < 0) {
              strcpy(error_string, "H5Dset_extent(recon_errors) error");
              H5Dclose(dataset);
	      H5Tclose(local_datatype);
              return(DM_FILEIO_FAILURE);
	    }

	    if ((status = H5Dwrite(dataset, local_datatype,
				   H5S_ALL,H5S_ALL,H5P_DEFAULT,
				   ptr_recon_errors->real_array)) < 0) {
	      strcpy(error_string, "Error in H5Dwrite (recon_errors)!");
	      H5Tclose(local_datatype);
	      H5Dclose(dataset);
	      return(DM_FILEIO_FAILURE);
	    }
	    
	    H5Dclose(dataset);
	    H5Sclose(dataspace);
	    H5Dclose(local_datatype);
	  }

          /* in this case we just want to update an existing itn array */
          if ((dataset = H5Dopen(h5_file_id,"/itn/itn_array")) < 0) {
              strcpy(error_string,"H5Dopen(itn_array) error");
              return(DM_FILEIO_FAILURE);
          }
    
          if ((file_dataspace = H5Dget_space(dataset)) < 0) {
              strcpy(error_string,"H5Dget_space(itn_array) error");
              H5Dclose(dataset);
              return(DM_FILEIO_FAILURE);
          }

          if ((n_dims = H5Sget_simple_extent_ndims(file_dataspace)) < 0) {
              strcpy(error_string,"H5Sget_simple_extent_ndims(itn_array) error");
              H5Dclose(dataset);
              H5Sclose(file_dataspace);
              return(DM_FILEIO_FAILURE);
          }

          /* First check if the new array is of the same dimensionality, and 
           * initialize the array_dims if so.
           */
          if (ptr_itn_array_struct->ny == 1) {
              if (n_dims != 2) {
                  sprintf(error_string,
                          "dm_h5_write_itn: New array must have dimensionality 2, not %d!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(file_dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              file_dims[0] = ptr_itn_array_struct->nx;
              file_dims[1] = 2;
              file_offsets[0] = 0;
              file_offsets[1] = 0;
#if USE_MPI
              memory_dims[0] = file_dims[0]/p;
              memory_dims[1] = file_dims[1];
              file_counts[0] = file_dims[0]/p;
              file_counts[1] = file_dims[1];
              slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
              memory_dims[0] = file_dims[0];
              memory_dims[1] = file_dims[1];
              file_counts[0] = memory_dims[0];
              file_counts[1] = memory_dims[1];
              slice_npix = ptr_itn_array_struct->npix;
#endif /* USE_MPI */
          } else if (ptr_itn_array_struct->nz == 1) {
              if (n_dims != 3) {
                  sprintf(error_string,
                          "dm_h5_write_itn: New array must have dimensionality 3, not %d!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(file_dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              file_dims[0] = ptr_itn_array_struct->ny;
              file_dims[1] = ptr_itn_array_struct->nx;
              file_dims[2] = 2;

              file_offsets[0] = 0;
              file_offsets[1] = 0;
              file_offsets[2] = 0;
#if USE_MPI
              memory_dims[0] = file_dims[0]/p;
              memory_dims[1] = file_dims[1];
              memory_dims[2] = file_dims[2];
              file_counts[0] = file_dims[0]/p;
              file_counts[1] = file_dims[1];
              file_counts[2] = file_dims[2];
              slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
              memory_dims[0] = file_dims[0];
              memory_dims[1] = file_dims[1];
              memory_dims[2] = file_dims[2];
              file_counts[0] = memory_dims[0];
              file_counts[1] = memory_dims[1];
              file_counts[2] = memory_dims[2];
              slice_npix = ptr_itn_array_struct->npix;
#endif /* USE_MPI */
          } else {
              if (n_dims != 4) {
                  sprintf(error_string,
                          "dm_h5_write_itn: New array must have dimensionality 4, not %d!",
                          n_dims);
                  H5Dclose(dataset);
                  H5Sclose(file_dataspace);
                  return(DM_FILEIO_FAILURE);
              }
              file_dims[0] = ptr_itn_array_struct->nz;
              file_dims[1] = ptr_itn_array_struct->ny;
              file_dims[2] = ptr_itn_array_struct->nx;
              file_dims[3] = 2;
      
              file_offsets[0] = 0; /* Will be adjusted for each slice */
              file_offsets[1] = 0;
              file_offsets[2] = 0;
              file_offsets[3] = 0;
#if USE_MPI
              memory_dims[0] = file_dims[0]/p;
              memory_dims[1] = file_dims[1];
              memory_dims[2] = file_dims[2];
              memory_dims[3] = file_dims[3];

              file_counts[0] = file_dims[0]/p;
              file_counts[1] = file_dims[1];
              file_counts[2] = file_dims[2];
              file_counts[3] = file_dims[3];
              slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
              memory_dims[0] = 1;
              memory_dims[1] = file_dims[1];
              memory_dims[2] = file_dims[2];
              memory_dims[3] = file_dims[3];

              file_counts[0] = 1;
              file_counts[1] = memory_dims[1];
              file_counts[2] = memory_dims[2];
              file_counts[3] = memory_dims[3];
              slice_npix = (dm_array_index_t)(ptr_itn_array_struct->nx) *
                  (dm_array_index_t)(ptr_itn_array_struct->ny);
#endif /* USE_MPI */
          }

          /* Now get the datatype. */
          if ((datatype = H5Dget_type(dataset)) < 0) {
              strcpy(error_string, "H5Dget_type(itn_array) error");
              H5Dclose(dataset);
              H5Sclose(file_dataspace);
              return(DM_FILEIO_FAILURE);
          }

          /*--- get order of type ---*/
          if ((order = H5Tget_order(datatype)) < 0) {
              strcpy(error_string,"H5Tget_order(itn_array) error");
              H5Dclose(dataset);
              H5Sclose(file_dataspace);
              H5Tclose(datatype);
              return(DM_FILEIO_FAILURE);
          }

#ifdef DM_ARRAY_DOUBLE
          local_datatype = H5Tcopy(H5T_NATIVE_DOUBLE);
#else
          local_datatype = H5Tcopy(H5T_NATIVE_FLOAT);
#endif

          /*--- get order of local type ---*/
          if ((local_order = H5Tget_order(local_datatype)) < 0) {
              strcpy(error_string,"H5Tget_order(itn_array) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              H5Sclose(file_dataspace);
              return(DM_FILEIO_FAILURE);
          }
    
          /*--- Convert to order if necessary so we can compare types ---*/
          if (local_order != order) {
              if ((status = H5Tset_order(local_datatype,order)) < 0) {
                  strcpy(error_string,"H5Tset_order(itn_array) error");
                  H5Dclose(dataset);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  H5Sclose(file_dataspace);
                  return(DM_FILEIO_FAILURE);
              } 
          } 

          if ((equal = H5Tequal(datatype,local_datatype)) < 1) {
              strcpy(error_string, 
                     "dm_h5_write_itn: Array of different type than existing array!");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              H5Sclose(file_dataspace);
              return(DM_FILEIO_FAILURE);
          }

          /*--- Convert back to local_order if necessary for writing ---*/
          if (local_order != order) {
              if ((status = H5Tset_order(local_datatype,local_order)) < 0) {
                  strcpy(error_string,"H5Tset_order(itn_array) error");
                  H5Dclose(dataset);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  H5Sclose(file_dataspace);
                  return(DM_FILEIO_FAILURE);
              } 
          } 
    
          /* Now see if we have to extend the dataset. */
          if ((status = H5Dset_extent(dataset,file_dims)) < 0) {
              strcpy(error_string, "H5Dset_extent(itn_array) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Sclose(file_dataspace);
              return(DM_FILEIO_FAILURE);
          }
          H5Sclose(file_dataspace);

          /* For some reason need to re-load the dataspace after extending */
          if ((file_dataspace = H5Dget_space(dataset)) < 0) {
              strcpy(error_string, "H5Dget_space(itn_array) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              return(DM_FILEIO_FAILURE);
          }
          
          if ((memory_dataspace =
               H5Screate_simple(n_dims, memory_dims, NULL)) < 0) {
              strcpy(error_string,"H5Screate_simple(itn_array,memory) error");
              H5Sclose(file_dataspace);
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              return(DM_FILEIO_FAILURE);
          }

      } /* endif(my_rank == 0) */
      
      /* Now here is where we have to be careful for split versus
       * interleaved complex arrays.  We take care of this by
       * always making an interleaved array.  Remember that
       * we should not use sizeof(dm_array_complex) because it
       * might be an array of pointers.
       */

#if USE_MPI
      /* Get the data from the nodes. npix will tell us the total number of
       * elements so we need to divide by the number of processes..
       */
      for (i = 0; i < p; i++) {
          if (i > 0) {
              if (my_rank == i) {
#if DM_ARRAY_SPLIT
                  /* Need to send separate real and imaginary arrays */
                  MPI_Send((ptr_itn_array_struct->complex_array)->re,
                           ptr_itn_array_struct->npix/p, MPI_ARRAY_REAL, 0, 99,
                           MPI_COMM_WORLD);
                  MPI_Send((ptr_itn_array_struct->complex_array)->im,
                           ptr_itn_array_struct->npix/p, MPI_ARRAY_REAL, 0, 98,
                           MPI_COMM_WORLD);
#else
                  /* Just send twice as many elements for interleaved */
                  MPI_Send(ptr_itn_array_struct->complex_array,
                           2*ptr_itn_array_struct->npix/p, MPI_ARRAY_REAL, 0, 99,
                           MPI_COMM_WORLD);
#endif /* DM_ARRAY_SPLIT */
              
              } /* endif(my_rank == i) */
          } /* endif(i > 0) */
          
          if (my_rank == 0) {
#if DM_ARRAY_SPLIT
              slice_array =
                  (dm_array_real *)malloc(2*sizeof(dm_array_real)*
                                          ptr_itn_array_struct->npix/p);
              
              if (i == 0) {
                  /* Directly map into one interleaved array */
                  for (ipix=0; ipix<ptr_itn_array_struct->npix/p; ipix++) {
                      *(slice_array+2*ipix) =
                          c_re(ptr_itn_array_struct->complex_array,ipix);
                      *(slice_array+2*ipix+1) =
                          c_im(ptr_itn_array_struct->complex_array,ipix);
                  } /* endfor */
              } else {
                  /* We will receive both im and re arrays and we need to map them
                   * into an interleaved array
                   */
                  temp_re = (dm_array_real *)malloc(sizeof(dm_array_real)*
                                                    ptr_itn_array_struct->npix/p);
                  temp_im = (dm_array_real *)malloc(sizeof(dm_array_real)*
                                                    ptr_itn_array_struct->npix/p);
                  
                  MPI_Recv(temp_re,ptr_itn_array_struct->npix/p,MPI_ARRAY_REAL,
                           i,99,MPI_COMM_WORLD, &mpi_status);
                  MPI_Recv(temp_im,ptr_itn_array_struct->npix/p,MPI_ARRAY_REAL,
                           i,98,MPI_COMM_WORLD, &mpi_status);
                  

                  /* Now map into one interleaved array */
                  for (ipix=0; ipix<ptr_itn_array_struct->npix/p; ipix++) {
                      *(slice_array+2*ipix) = *(temp_re+ipix);
                      *(slice_array+2*ipix+1) =*(temp_im+ipix);
                  } /* endfor */
                  
                  free(temp_re);
                  free(temp_im);
              } /* endif(i == 0) */
#else
              /* We will receive an interleaved array */
              slice_array =
                  (dm_array_real *)malloc(2*sizeof(dm_array_real)*
                                          ptr_itn_array_struct->npix/p);
              if (i == 0) {
                  /* Directly map into one interleaved array */
                  for (ipix=0; ipix<ptr_itn_array_struct->npix/p; ipix++) {
                      *(slice_array+2*ipix) =
                          c_re(ptr_itn_array_struct->complex_array,ipix);
                      *(slice_array+2*ipix+1) =
                          c_im(ptr_itn_array_struct->complex_array,ipix);
                  } /* endfor */
                  
              } else {
                  MPI_Recv(slice_array,2*ptr_itn_array_struct->npix/p,
                           MPI_ARRAY_REAL,
                           i,99,MPI_COMM_WORLD, &mpi_status);
              } /* endif(i == 0) */
              
#endif /* DM_ARRAY_SPLIT */

              /* Determine the file_offsets here. Note that we have to
               * divide by
               * number of processes. Note that we always have at least 2
               * dimensions because it is complex.
               */
              if (n_dims == 2) {
                  file_offsets[0] = i*ptr_itn_array_struct->nx/p;
              } else if (n_dims == 3) {
                  file_offsets[0] = i*ptr_itn_array_struct->ny/p;
              } else if (n_dims == 4) {
                  file_offsets[0] = i*ptr_itn_array_struct->nz/p;
              }
              
              if ((status =
                   H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,
                                                file_offsets, NULL,
                                                file_counts, NULL)) < 0) {
                  strcpy(error_string,
                         "Error in H5Sselect_hyperslab(itn_array)");
                  H5Sclose(file_dataspace);
                  H5Sclose(memory_dataspace);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  free(slice_array);
                  return(DM_FILEIO_FAILURE);
              }
              
              if ((status = H5Dwrite(dataset, local_datatype,
                                     memory_dataspace,
                                     file_dataspace,H5P_DEFAULT,
                                     slice_array)) < 0) {
                  strcpy(error_string, "Error in H5Dwrite (itn_array)!");
                  H5Sclose(file_dataspace);
                  H5Sclose(memory_dataspace);
                  H5Tclose(datatype);
                  H5Tclose(local_datatype);
                  free(slice_array);
                  return(DM_FILEIO_FAILURE);
              }
              
              free(slice_array);
          }  /* endif(my_rank == 0) */ 
      } /* endfor */

#else /* no USE_MPI */
      
      slice_array =
          (dm_array_real *)malloc(2*sizeof(dm_array_real)*slice_npix);
      if (slice_array == NULL) {
          strcpy(error_string,"slice malloc() error");
          H5Dclose(dataset);
          H5Sclose(file_dataspace);
          H5Sclose(memory_dataspace);
          H5Tclose(datatype);
          H5Tclose(local_datatype);
          return(DM_FILEIO_FAILURE);
      }
      for (iz=0; iz<(ptr_itn_array_struct->nz); iz++) {
          file_offsets[0] = iz;
          zoffset = iz*slice_npix;
          for (ipix=0; ipix<slice_npix; ipix++) {
              *(slice_array+2*ipix) = 
                  c_re(ptr_itn_array_struct->complex_array,(ipix+zoffset));
              *(slice_array+2*ipix+1) =
                  c_im(ptr_itn_array_struct->complex_array,(ipix+zoffset));
          }
          
          if ((status = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,
                                            file_offsets,NULL,
                                            file_counts,NULL)) < 0) {
              strcpy(error_string,"Error in H5Sselect_hyperslab(itn_array)");
              H5Dclose(dataset);
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              free(slice_array);
              H5Tclose(local_datatype);
              return(DM_FILEIO_FAILURE);
          }
          if ((status = H5Dwrite(dataset,local_datatype,memory_dataspace,
                                 file_dataspace,H5P_DEFAULT,slice_array)) < 0) {
              strcpy(error_string,"Error in H5Dwrite(itn_array)");
              H5Dclose(dataset);
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              free(slice_array);
              H5Tclose(local_datatype);
              return(DM_FILEIO_FAILURE);
          }
      }

      free(slice_array);
#endif /* USE_MPI */

      if (my_rank == 0) {
          H5Dclose(dataset);
          H5Sclose(file_dataspace);
          H5Sclose(memory_dataspace);
          H5Tclose(datatype);
          H5Tclose(local_datatype);
      } /* endif(my_rank == 0) */
      
      return(DM_FILEIO_SUCCESS);
  }
}

/*-------------------------------------------------------------------------*/
int dm_h5_adi_group_exists(hid_t h5_file_id,int my_rank)
{
  hid_t adi_group;
  int exists;
  
  if (my_rank == 0) {
      adi_group = H5Gopen(h5_file_id,"/adi");
      H5Gclose(adi_group);
      if (adi_group < 0) {
          exists = 0;
      } else {
          exists = 1;
      }
  } /* endif(my_rank == 0) */

#if USE_MPI
   MPI_Bcast(&exists,1,MPI_INT,0,MPI_COMM_WORLD);
#endif
      
  if (exists == 0) {
    return(0);
  } else {
    return(1);
  }
}

/*-------------------------------------------------------------------------*/
int dm_h5_ads_group_exists(hid_t h5_file_id,int my_rank)
{
  hid_t ads_group;
  int exists;

  if (my_rank == 0) {
      ads_group = H5Gopen(h5_file_id,"/ads");
      H5Gclose(ads_group);

      if (ads_group < 0) {
          exists = 0;
      } else {
          exists = 1;
      }
  } /* endif(my_rank == 0) */

#if USE_MPI
   MPI_Bcast(&exists,1,MPI_INT,0,MPI_COMM_WORLD);
#endif
   
  if (exists == 0) {
    return(0);
  } else {
    return(1);
  }
}

/*-------------------------------------------------------------------------*/
int dm_h5_spt_group_exists(hid_t h5_file_id,int my_rank)
{
  hid_t spt_group;
  int exists;

  if (my_rank == 0) {
      spt_group = H5Gopen(h5_file_id,"/spt");
      H5Gclose(spt_group);
      
      if (spt_group < 0) {
          exists = 0;
      } else {
          exists = 1;
      }
  } /* endif(my_rank == 0) */
  
#if USE_MPI
  MPI_Bcast(&exists,1,MPI_INT,0,MPI_COMM_WORLD);
#endif
  
  if (exists == 0) {
      return(0);
  } else {
      return(1);
  }
}

/*-------------------------------------------------------------------------*/
int dm_h5_itn_group_exists(hid_t h5_file_id,int my_rank)
{
    hid_t itn_group;
    int exists;

    if (my_rank == 0) {
        itn_group = H5Gopen(h5_file_id,"/itn");
        H5Gclose(itn_group);
        
        if (itn_group < 0) {
            exists = 0;
        } else {
            exists = 1;
        }
    } /* endif(my_rank == 0) */
    
#if USE_MPI
    MPI_Bcast(&exists,1,MPI_INT,0,MPI_COMM_WORLD);
#endif
    
    if (exists == 0) {
        return(0);
    } else {
        return(1);
    }
}

/*-------------------------------------------------------------------------*/
int dm_h5_comments_group_exists(hid_t h5_file_id,int my_rank)
{
    hid_t comments_group;
    int exists;

    if (my_rank == 0) {
        comments_group = H5Gopen(h5_file_id,"/comments");
        
        if (comments_group < 0) {
            exists = 0;
        } else {
            exists = 1;
	    H5Gclose(comments_group);
        }
    } /* endif(my_rank == 0) */
    
#if USE_MPI
    MPI_Bcast(&exists,1,MPI_INT,0,MPI_COMM_WORLD);
#endif
    
    if (exists == 0) {
        return(0);
    } else {
        return(1);
    }
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_comments_info(hid_t h5_file_id,
			     int *ptr_n_strings, int *ptr_string_length,
			     char *error_string,
                             int my_rank)
{
  hid_t comments_group;
  hid_t dataset;
  herr_t status;                             

  strcpy(error_string,"");

  if (my_rank == 0) {

      if ((comments_group = H5Gopen(h5_file_id,"/comments")) < 0) {
          strcpy(error_string,"H5Gopen(\"/comments\") error");
          return(DM_FILEIO_FAILURE);
      }
      
      if ((dataset = H5Dopen(comments_group,"comment_string_length")) < 0) {
          strcpy(error_string,"H5Dopen(comment_string_length) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            ptr_string_length)) < 0) {
          sprintf(error_string,"H5Dread(comment_string_length) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      
      if ((dataset = H5Dopen(comments_group,"n_comment_strings")) < 0) {
          strcpy(error_string,"H5Dopen(n_comment_strings) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            ptr_n_strings)) < 0) {
          sprintf(error_string,"H5Dread(n_comment_strings) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      H5Gclose(comments_group);
  } /* endif(my_rank == 0) */
  
    /* Send info on strlen and number of comments to other processes
     * if necesary
     */
#if USE_MPI
  MPI_Bcast(ptr_string_length,1,MPI_INT,0,MPI_COMM_WORLD);
  MPI_Bcast(ptr_n_strings,1,MPI_INT,0,MPI_COMM_WORLD);
#endif /* USE_MPI */

      
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_comments(hid_t h5_file_id,
			dm_comment_struct *ptr_comment_struct,
			char *error_string,
                        int my_rank)
{
  hid_t comments_group, xfer_pid;
  hid_t dataset, datatype, dataspace;
  herr_t status;                             
  int i_string, local_string_length, local_n_strings, check;
  char *local_string;

  strcpy(error_string,"");

  if (my_rank == 0) {
      hvl_t local_string_array[ptr_comment_struct->n_strings_max];

      if ((comments_group = H5Gopen(h5_file_id,"/comments")) < 0) {
          strcpy(error_string,"H5Gopen(\"/comments\") error");
          return(DM_FILEIO_FAILURE);
      }

      if ((dataset = H5Dopen(comments_group,"comment_string_length")) < 0) {
          strcpy(error_string,"H5Dopen(comment_string_length) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            &local_string_length)) < 0) {
          sprintf(error_string,"H5Dread(comment_string_length) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      
      if ((dataset = H5Dopen(comments_group,"n_comment_strings")) < 0) {
          strcpy(error_string,"H5Dopen(n_comment_strings) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            &local_n_strings)) < 0) {
          sprintf(error_string,"H5Dread(n_comment_strings) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      
      if (local_string_length > (ptr_comment_struct->string_length)) {
          sprintf(error_string,"Comment string length is %d but your max is %d",
                  local_string_length,(ptr_comment_struct->string_length));
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if (local_n_strings > (ptr_comment_struct->n_strings_max)) {
          sprintf(error_string,"Number of comment strings is %d but your max is %d",
                  local_n_strings,(ptr_comment_struct->n_strings_max));
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      dm_clear_comments(ptr_comment_struct);
      
      if ((dataset = H5Dopen(comments_group,"comment_strings")) < 0) {
          strcpy(error_string,"H5Dopen(comment_strings) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((datatype = H5Dget_type(dataset)) < 0) {
          strcpy(error_string,"H5Dget_type(comment_strings) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((xfer_pid = H5Pcreate(H5P_DATASET_XFER)) < 0) {
          strcpy(error_string,"H5Pcreate(comment_strings) error");
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Dget_space(comment_strings) error");
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(comments_group);
          H5Pclose(xfer_pid);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,xfer_pid,
                            local_string_array)) < 0) {
          strcpy(error_string,"H5Dread(comment_strings) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          H5Gclose(comments_group);
          H5Sclose(dataspace);
          H5Pclose(xfer_pid);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      
      /* Now that we have a copy of the string array, copy it into the
       * comment_struct.  We do it this way in case there's a mismatch
       * between string length in the file, and string length in comment_struct.
       */
      for (i_string=0; i_string<local_n_strings; i_string++) {
          if (local_string_array[i_string].len != 0) {
              dm_add_string_to_comments((char *)local_string_array[i_string].p,
                                        ptr_comment_struct);
          }
      }
      
      if ((status = 
           H5Dvlen_reclaim(datatype,dataspace,
                           xfer_pid,local_string_array)) < 0) {
          strcpy(error_string,"H5Dvlen_reclaim(comment_strings) error");
          H5Tclose(datatype);
          H5Sclose(dataspace);
          H5Gclose(comments_group);
          H5Pclose(xfer_pid);
          return(DM_FILEIO_FAILURE);
      }
      H5Pclose(xfer_pid);
      H5Tclose(datatype);
      H5Sclose(dataspace);
      
      /* 
       * Now read the single strings. Allocate memory in local_string
       */
      local_string = (char *)malloc(local_string_length*sizeof(char));
      
      if ((dataset = H5Dopen(comments_group,"specimen_name")) < 0) {
          strcpy(error_string,"H5Dopen(specimen_name) error");
          free(local_string);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((datatype = H5Dget_type(dataset)) < 0) {
          strcpy(error_string,"H5Dget_type(specimen_name) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            local_string)) < 0) {
          strcpy(error_string,"H5Dread(specimen_name) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          free(local_string);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      H5Tclose(datatype);
      dm_add_specimen_name_to_comments(local_string, ptr_comment_struct);
      free(local_string);
      
      local_string = (char *)malloc(local_string_length*sizeof(char));
      if ((dataset = H5Dopen(comments_group,"collection_date")) < 0) {
          strcpy(error_string,"H5Aopen(collection_date) error");
          free(local_string);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((datatype = H5Dget_type(dataset)) < 0) {
          strcpy(error_string,"H5Dget_type(collection_date) error");
          H5Dclose(dataset);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            local_string)) < 0) {
          strcpy(error_string,"H5Aread(collection_date) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          free(local_string);
          H5Gclose(comments_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      H5Tclose(datatype);
      H5Gclose(comments_group);
      dm_add_collection_date_to_comments(local_string,ptr_comment_struct);
      
      free(local_string);

  } /* endif(my_rank == 0) */
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_ainfo_info(hid_t h5_file_id,
			  int *ptr_n_frames, int *ptr_string_length,
			  char *error_string,
                          int my_rank)
{
  hid_t ainfo_group, attr;
  hid_t dataset,datatype;
  herr_t status;  
  int dm_ainfo_version;

  strcpy(error_string,"");

  if (my_rank == 0) {

      if ((ainfo_group = H5Gopen(h5_file_id,"/ainfo")) < 0) {
          strcpy(error_string,"H5Gopen(\"/ainfo\") error");
          return(DM_FILEIO_FAILURE);
      }
  
      if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(datatype) error");
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }
  
      H5Tset_size(datatype,H5_SIZEOF_INT);
      if ((attr = H5Aopen_name(ainfo_group,"ainfo_version")) < 0) {
          strcpy(error_string,"H5Aopen_name(ainfo_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Aread(attr,datatype,
                            &dm_ainfo_version)) < 0) {
          strcpy(error_string,"H5Aread(ainfo_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Aclose(attr);
      H5Tclose(datatype);
  
      if (dm_ainfo_version > DM_AINFO_VERSION) {
          sprintf(error_string,
                  "Can only handle AINFO version up to %d, not %d",
                  DM_AINFO_VERSION,dm_ainfo_version);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
   
      if ((dataset = H5Dopen(ainfo_group,"string_length")) < 0) {
          strcpy(error_string,"H5Dopen(string_length) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            ptr_string_length)) < 0) {
          sprintf(error_string,"H5Dread(string_length) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);

      if ((dataset = H5Dopen(ainfo_group,"n_frames")) < 0) {
          strcpy(error_string,"H5Dopen(n_frames) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            ptr_n_frames)) < 0) {
          sprintf(error_string,"H5Dread(n_frames) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      H5Gclose(ainfo_group);

  } /* endif(my_rank = 0) */

  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_ainfo(hid_t h5_file_id,
		     dm_ainfo_struct *ptr_ainfo_struct,
		     char *error_string,
                     int my_rank)
{
  hid_t ainfo_group, attr;
  hid_t dataset, datatype;
  herr_t status;                             
  int i_string, dm_ainfo_version, this_value;
  size_t local_string_length, local_n_frames;
  char *local_string_array;
  double *local_double_array;

  strcpy(error_string,"");

  if (my_rank == 0) {

      if ((ainfo_group = H5Gopen(h5_file_id,"/ainfo")) < 0) {
          strcpy(error_string,"H5Gopen(\"/ainfo\") error");
          return(DM_FILEIO_FAILURE);
      }
   
      if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(datatype) error");
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }
  
      H5Tset_size(datatype,H5_SIZEOF_INT);
      if ((attr = H5Aopen_name(ainfo_group,"ainfo_version")) < 0) {
          strcpy(error_string,"H5Aopen_name(ainfo_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Aread(attr,datatype,
                            &dm_ainfo_version)) < 0) {
          strcpy(error_string,"H5Aread(ainfo_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Aclose(attr);
      H5Tclose(datatype);
  
      if (dm_ainfo_version > DM_AINFO_VERSION) {
          sprintf(error_string,
                  "Can only handle AINFO version up to %d, not %d",
                  DM_AINFO_VERSION,dm_ainfo_version);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }

      if ((dataset = H5Dopen(ainfo_group,"string_length")) < 0) {
          strcpy(error_string,"H5Dopen(string_length) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            &local_string_length)) < 0) {
          sprintf(error_string,"H5Dread(string_length) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
  
      if ((dataset = H5Dopen(ainfo_group,"n_frames")) < 0) {
          strcpy(error_string,"H5Dopen(n_frames) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            &local_n_frames)) < 0) {
          sprintf(error_string,"H5Dread(n_frames) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);

      if (local_string_length > (ptr_ainfo_struct->string_length)) {
          sprintf(error_string,"Ainfo string length is %d but your max is %d",
                  local_string_length,(ptr_ainfo_struct->string_length));
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }

      if (local_n_frames > (ptr_ainfo_struct->n_frames_max)) {
          sprintf(error_string,"Number of filenames is %d but your max is %d",
                  local_n_frames,(ptr_ainfo_struct->n_frames_max));
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }

      dm_clear_ainfo(ptr_ainfo_struct);

      /* Now we can copy the local_n_frames to the ainfo_struct */
      ptr_ainfo_struct->n_frames = local_n_frames;

      /* Now read in the other integers*/
      if ((dataset = H5Dopen(ainfo_group,"no_background")) < 0) {
          strcpy(error_string,"H5Dopen(no_background) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            &this_value)) < 0) {
          sprintf(error_string,"H5Dread(no_background) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      ptr_ainfo_struct->no_background = this_value;

      if ((dataset = H5Dopen(ainfo_group,"dk_by_pix")) < 0) {
          strcpy(error_string,"H5Dopen(dk_by_pix) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            &this_value)) < 0) {
          sprintf(error_string,"H5Dread(dk_by_pix) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      ptr_ainfo_struct->dk_by_pix = this_value;

      if ((dataset = H5Dopen(ainfo_group,"merge_first")) < 0) {
          strcpy(error_string,"H5Dopen(merge_first) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_INT,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            &this_value)) < 0) {
          sprintf(error_string,"H5Dread(merge_first) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      ptr_ainfo_struct->merge_first = this_value;

      if ((datatype = H5Tcopy(H5T_C_S1)) < 0) {
          strcpy(error_string,"H5Tcopy(filename_array) error");
          H5Tclose(datatype);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Tset_size(datatype,local_string_length);
      local_string_array = 
          (char *)malloc(local_string_length*local_n_frames);
      if ((dataset = H5Dopen(ainfo_group,"filename_array")) < 0) {
          strcpy(error_string,"H5Aopen(filename_array) error");
          free(local_string_array);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            local_string_array)) < 0) {
          strcpy(error_string,"H5Aread(filename_array) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          free(local_string_array);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      /* Now that we have a copy of the filename array, copy it into the
       * ainfo_struct.  We do it this way in case there's a mismatch
       * between string length in the file, and string length in ainfo_struct.
       */
      for (i_string=0; i_string<local_n_frames; i_string++) {
          dm_add_filename_to_ainfo((local_string_array+
                                    i_string*local_string_length),
                                   ptr_ainfo_struct);
      }
      /* Clear the local_string_array for the systime array to be read */
      dm_clear_local_string_array(local_string_array, local_string_length,
                                  local_n_frames);
  
      if ((dataset = H5Dopen(ainfo_group,"systime_array")) < 0) {
          strcpy(error_string,"H5Aopen(systime_array) error");
          free(local_string_array);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            local_string_array)) < 0) {
          strcpy(error_string,"H5Aread(systime_array) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          free(local_string_array);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      /* Now that we have a copy of the systime array, copy it into the
       * ainfo_struct.  We do it this way in case there's a mismatch
       * between string length in the file, and string length in ainfo_struct.
       */
      for (i_string=0; i_string<local_n_frames; i_string++) {
          dm_add_systime_to_ainfo((local_string_array+
                                   i_string*local_string_length),
                                  ptr_ainfo_struct);
      }
      /* Clear the local_systime_array for the file_directory to be read in */
      dm_clear_local_string_array(local_string_array, local_string_length,
                                  local_n_frames);

      if ((dataset = H5Dopen(ainfo_group,"file_directory")) < 0) {
          strcpy(error_string,"H5Aopen(file_directory) error");
          free(local_string_array);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            local_string_array)) < 0) {
          strcpy(error_string,"H5Aread(file_directory) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          free(local_string_array);
          H5Gclose(ainfo_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      dm_add_file_directory_to_ainfo(local_string_array,
                                     ptr_ainfo_struct);
      H5Tclose(datatype);
      free(local_string_array);
  
      /* Now we start reading the double arrays */
      local_double_array = (double *)malloc(local_n_frames*sizeof(double));
  
      if ((dataset = H5Dopen(ainfo_group,"theta_x_radians_array")) < 0) {
          strcpy(error_string,"H5Aopen(theta_x_radians_array) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          free(local_double_array);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_DOUBLE,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            local_double_array)) < 0) {
          strcpy(error_string,"H5Aread(theta_x_radians_array) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          free(local_double_array);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);

      /* Now copy the values to ainfo_struct */
      for (i_string=0; i_string<local_n_frames; i_string++) {
          *(ptr_ainfo_struct->theta_x_radians_array +i_string) = 
              *(local_double_array + i_string);
      }

      if ((dataset = H5Dopen(ainfo_group,"xcenter_offset_pixels_array")) < 0) {
          strcpy(error_string,"H5Aopen(xcenter_offset_pixels_array) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          free(local_double_array);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_DOUBLE,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            local_double_array)) < 0) {
          strcpy(error_string,"H5Aread(xcenter_offset_pixels_array) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          free(local_double_array);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      /* Now copy the values to ainfo_struct */
      for (i_string=0; i_string<local_n_frames; i_string++) {
          *(ptr_ainfo_struct->xcenter_offset_pixels_array +i_string) = 
              *(local_double_array + i_string);
      }

      if ((dataset = H5Dopen(ainfo_group,"ycenter_offset_pixels_array")) < 0) {
          strcpy(error_string,"H5Aopen(ycenter_offset_pixels_array) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          free(local_double_array);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Dread(dataset,H5T_NATIVE_DOUBLE,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            local_double_array)) < 0) {
          strcpy(error_string,"H5Aread(ycenter_offset_pixels_array) error");
          H5Dclose(dataset);
          H5Gclose(ainfo_group);
          free(local_double_array);
          return(DM_FILEIO_FAILURE);
      }
      H5Dclose(dataset);
      /* Now copy the values to ainfo_struct */

      for (i_string=0; i_string<local_n_frames; i_string++) {
          *(ptr_ainfo_struct->ycenter_offset_pixels_array +i_string) = 
              *(local_double_array + i_string);
      }  

      free(local_double_array);
      H5Gclose(ainfo_group);
  } /* endif(my_rank == 0) */
  
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_adi_info(hid_t h5_file_id,
			int *ptr_nx, int *ptr_ny, int *ptr_nz,
			int *ptr_error_is_present,
			dm_adi_struct *ptr_adi_struct,
			char *error_string,
                        int my_rank)
{
  hid_t adi_group;
  hid_t datatype, dataspace, dataset;
  hid_t attr;
  hsize_t *arr_dims;
  herr_t status;                             
  int arr_n_dims, dm_adi_version;
  int error_nx, error_ny, error_nz;

  strcpy(error_string,"");

  if (my_rank == 0) {

    if ((adi_group = H5Gopen(h5_file_id,"/adi")) < 0) {
      strcpy(error_string,"H5Gopen(\"/adi\") error");
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
    }
    
    if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
      strcpy(error_string,"H5Tcopy(datatype) error");
      H5Tclose(datatype);
      return(DM_FILEIO_FAILURE);
    }
    H5Tset_size(datatype,H5_SIZEOF_INT);
    if ((attr = H5Aopen_name(adi_group,"adi_version")) < 0) {
      strcpy(error_string,"H5Aopen_name(adi_version) error");
      H5Aclose(attr);
      H5Tclose(datatype);
      H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
    }
    if ((status = H5Aread(attr,datatype,
			  &dm_adi_version)) < 0) {
      strcpy(error_string,"H5Aread(adi_version) error");
      H5Aclose(attr);
      H5Tclose(datatype);
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
      }
    H5Aclose(attr);
    H5Tclose(datatype);
    
    if (dm_adi_version > DM_ADI_VERSION) {
      sprintf(error_string,
	      "Can only handle ADI version up to %d, not %d",
	      DM_ADI_VERSION,dm_adi_version);
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
    } 
  
    /*--- check on dimensions of adi_array --- */
    if ((dataset = H5Dopen(adi_group,"adi_array")) < 0) {
      strcpy(error_string,"H5Dopen(\"adi_array\") error");
      H5Dclose(dataset);
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
    }

    if ((dataspace = H5Dget_space(dataset)) < 0) {
      strcpy(error_string,"H5Dget_space(adi_array) error");
      H5Sclose(dataspace);
      H5Dclose(dataset);
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
    }
    arr_n_dims = H5Sget_simple_extent_ndims(dataspace);
    
    arr_dims = (hsize_t *)malloc(arr_n_dims*sizeof(hsize_t));
    if ((status = H5Sget_simple_extent_dims(dataspace,arr_dims,NULL)) < 0) {
      strcpy(error_string,"H5Dget_simple_extent_dims(adi_array) error");
      free(arr_dims);
      H5Sclose(dataspace);
      H5Dclose(dataset);
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
    }
    
    if (arr_n_dims == 1) {
      *ptr_nx = (*(arr_dims+0));
      *ptr_ny = 1;
      *ptr_nz = 1;
    } else if (arr_n_dims == 2) {
      *ptr_ny = (*(arr_dims+0));
      *ptr_nx = (*(arr_dims+1));
      *ptr_nz = 1;
    } else if (arr_n_dims == 3) {
      *ptr_nz = (*(arr_dims+0));
      *ptr_ny = (*(arr_dims+1));
      *ptr_nx = (*(arr_dims+2));
    } else {
      sprintf(error_string,"Error: ADI array n_dims=%d rather than 1, 2, or 3",
	      (int)arr_n_dims);
      free(arr_dims);
      H5Sclose(dataspace);
      H5Dclose(dataset);
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
    }    
    free(arr_dims); 
    H5Sclose(dataspace);
    H5Dclose(dataset);

  } /* endif(my_rank == 0) */
  
  /* Send info on array size to other processes if necesary */
#if USE_MPI
  MPI_Bcast(ptr_nx,1,MPI_INT,0,MPI_COMM_WORLD);
  MPI_Bcast(ptr_ny,1,MPI_INT,0,MPI_COMM_WORLD);
  MPI_Bcast(ptr_nz,1,MPI_INT,0,MPI_COMM_WORLD);
#endif /* USE_MPI */
  
  /*--- check on dimensions of adi_error_array ---*/
  *ptr_error_is_present = 0;

  if (my_rank == 0) {
      if ((dataset = H5Dopen(adi_group,"adi_error_array")) < 0) {
          H5Dclose(dataset);
      } else {
          if ((datatype = H5Dget_type(dataset)) < 0) {
              strcpy(error_string,"H5Dget_type(adi_error_array) error");
              H5Tclose(datatype);
              H5Dclose(dataset);
              H5Gclose(adi_group);
              return(DM_FILEIO_FAILURE);
          }
          if ((dataspace = H5Dget_space(dataset)) < 0) {
              strcpy(error_string,"H5Dget_space(adi_error_array) error");
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Dclose(dataset);
              H5Gclose(adi_group);
              return(DM_FILEIO_FAILURE);
          }
          arr_n_dims = H5Sget_simple_extent_ndims(dataspace);
          arr_dims = (hsize_t *)malloc(arr_n_dims*sizeof(hsize_t));
          if ((status = H5Sget_simple_extent_dims(dataspace,arr_dims,
                                                  NULL)) < 0) {
              strcpy(error_string,"H5Dget_simple_extent_dims(adi_error_array) error");
              free(arr_dims);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Dclose(dataset);
              H5Gclose(adi_group);
              return(DM_FILEIO_FAILURE);
          }
    
          if (arr_n_dims == 1) {
              error_nx = (*(arr_dims+0));
              error_ny = 1;
              error_nz = 1;
          } else if (arr_n_dims == 2) {
              error_ny = (*(arr_dims+0));
              error_nx = (*(arr_dims+1));
              error_nz = 1;
          } else if (arr_n_dims == 3) {
              error_nz = (*(arr_dims+0));
              error_ny = (*(arr_dims+1));
              error_nx = (*(arr_dims+2));
          } else {
              sprintf(error_string,
                      "Error: adi_error_array n_dims=%d rather than 1, 2, or 3",
                      (int)arr_n_dims);
              free(arr_dims);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Dclose(dataset);
              H5Gclose(adi_group);
              return(DM_FILEIO_FAILURE);
          }
          free(arr_dims);

          if ((error_nx != (*ptr_nx)) || (error_ny != (*ptr_ny)) ||
              (error_nz != (*ptr_nz))) {
              sprintf(error_string,
                      "Error: adi_array[%d,%d,%d] but adi_error_array[%d,%d,%d]",
                      (int)(*ptr_nx),(int)(*ptr_ny),(int)(*ptr_nz),
                      error_nx,error_ny,error_nz);
              H5Sclose(dataspace);
              H5Tclose(datatype);
              H5Dclose(dataset);
              return(DM_FILEIO_FAILURE);
          }
  
          /* If we got to this end point, then all is OK with adi_error_array */
          *ptr_error_is_present = 1;

      } /* endif(H5Dopen(adi_group,"adi_error_array") == 0) */
  } /* endif(my_rank == 0) */

  /* Let other processes know if we have error array or not. */
#if USE_MPI
  MPI_Bcast(ptr_error_is_present,1,MPI_INT,0,MPI_COMM_WORLD);
#endif

  if (my_rank == 0) {
      H5Sclose(dataspace);
      H5Tclose(datatype);
      H5Dclose(dataset);
  } /* endif(my_rank == 0) */
  
  /*----------------------------------------------------------------*/
  /* Now get adi_struct, for now only read-out to root process */

  if (my_rank == 0) {
      if ((dataset = H5Dopen(adi_group,"adi_struct")) < 0) {
          strcpy(error_string,"H5Dopen(adi_struct) error");
          H5Dclose(dataset);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Gget_space(adi_struct) error");
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((datatype = H5Tcreate(H5T_COMPOUND,
                                sizeof(dm_adi_struct))) < 0) {
          strcpy(error_string,"H5Tcreate(adi_struct) error");
          H5Tclose(datatype);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
      dm_h5_insert_adi_struct_members(datatype);
      
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            ptr_adi_struct)) < 0) {
          strcpy(error_string,"H5Dread(adi_struct) error");
          H5Tclose(datatype);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Tclose(datatype);
      H5Sclose(dataspace);
      H5Dclose(dataset);
      H5Gclose(adi_group);

      } /* endif(my_rank == 0) */
  
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_adi(hid_t h5_file_id,
		   dm_array_real_struct *ptr_adi_array_struct,
		   dm_array_real_struct *ptr_adi_error_array_struct,
		   char *error_string,
                   int my_rank,
                   int p)
{
  hid_t adi_group;
  hid_t datatype, memspace, dataset, dataspace;
  hid_t mem_type_id;
  hid_t attr, local_datatype, read_datatype;
  hsize_t *arr_dims;
  herr_t status;                             
  int arr_n_dims, dm_adi_version;
  int local_nx, local_ny, local_nz;
  int error_exists, no_error;
  H5T_order_t order,local_order;
#if USE_MPI
  hsize_t *file_offsets, *file_counts;
  int i;
  dm_array_real *slice_array;
  MPI_Status mpi_status;
#endif

  if (my_rank == 0) {
      strcpy(error_string,"");
      
      if ((adi_group = H5Gopen(h5_file_id,"/adi")) < 0) {
          strcpy(error_string,"H5Gopen(\"/adi\") error");
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(datatype) error");
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Tset_size(datatype,H5_SIZEOF_INT);
      if ((attr = H5Aopen_name(adi_group,"adi_version")) < 0) {
          strcpy(error_string,"H5Aopen_name(adi_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Aread(attr,datatype,
                            &dm_adi_version)) < 0) {
          strcpy(error_string,"H5Aread(comment_string_length) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Aclose(attr);
      H5Tclose(datatype);
      
      if (dm_adi_version > DM_ADI_VERSION) {
          sprintf(error_string,
                  "Can only handle ADI version up to %d, not %d",
                  DM_ADI_VERSION,dm_adi_version);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }


  
      /*--- adi_array ---*/
      if ((dataset = H5Dopen(adi_group,"adi_array")) < 0) {
          strcpy(error_string,"H5Dopen(\"adi_array\") error");
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((datatype = H5Dget_type(dataset)) < 0) {
          strcpy(error_string,"H5Dget_type(adi_array) error");
          H5Dclose(dataset);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }

      /*--- get order of type ---*/
      if ((order = H5Tget_order(datatype)) < 0) {
          strcpy(error_string,"H5Tget_order(adi_array) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }

      /*--- create local datatype to compare the orders ---*/
      if ((local_datatype = H5Tcopy(H5T_NATIVE_FLOAT)) < 0) {
          strcpy(error_string,"H5Tcopy(adi_array) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }

      /*--- get order of local type ---*/
      if ((local_order = H5Tget_order(local_datatype)) < 0) {
          strcpy(error_string,"H5Tget_order(adi_array) error");
          H5Dclose(dataset);
          H5Tclose(datatype);
          H5Gclose(adi_group);
          H5Tclose(local_datatype);
          return(DM_FILEIO_FAILURE);
      }
 
      /*--- Convert to local order if necessary ---*/
      read_datatype = H5Tcopy(datatype);
      if (local_order != order) {
          if ((status = H5Tset_order(read_datatype,local_order)) < 0) {
              strcpy(error_string,"H5Tset_order(adi_array) error");
              H5Dclose(dataset);
              H5Tclose(datatype);
              H5Tclose(local_datatype);
              H5Gclose(adi_group);
              return(DM_FILEIO_FAILURE);
          } 
      }
      H5Tclose(local_datatype);
      H5Tclose(datatype);

      if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Dget_space(adi_array) error");
          H5Sclose(dataspace);
          H5Tclose(read_datatype);
          H5Dclose(dataset);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }

      arr_n_dims = H5Sget_simple_extent_ndims(dataspace);
      arr_dims = (hsize_t *)malloc(arr_n_dims*sizeof(hsize_t));
      if ((status = H5Sget_simple_extent_dims(dataspace,arr_dims,NULL)) < 0) {
          strcpy(error_string,"H5Dget_simple_extent_dims(adi_array) error");
          free(arr_dims);
          H5Sclose(dataspace);
          H5Tclose(read_datatype);
          H5Dclose(dataset);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }
    
      if (arr_n_dims == 1) {
          local_nx = (*(arr_dims+0));
          local_ny = 1;
          local_nz = 1;
#if USE_MPI
          file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
          
          /* Note that we have to divide by the number of processes here.
           */
          file_counts = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_counts + 0) = (*(arr_dims+0))/p;
#endif
      } else if (arr_n_dims == 2) {
          local_ny = (*(arr_dims+0));
          local_nx = (*(arr_dims+1));
          local_nz = 1;
#if USE_MPI
          file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_offsets + 0) = 0;
          *(file_offsets + 1) = 0; /* Will be adjusted for each slice */
          
          /* Note that we have to divide by the number of processes here.
           */
          file_counts = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_counts + 0) = (*(arr_dims+0))/p;
          *(file_counts + 1) = (*(arr_dims+1));
#endif
      } else if (arr_n_dims == 3) {
          local_nz = (*(arr_dims+0));
          local_ny = (*(arr_dims+1));
          local_nx = (*(arr_dims+2));
#if USE_MPI
          file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_offsets + 0) = 0;
          *(file_offsets + 1) = 0;
          *(file_offsets + 2) = 0; /* Will be adjusted for each slice */
           
          /* Note that we have to divide by the number of processes here.
           */
          file_counts = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_counts + 0) = (*(arr_dims+0))/p;
          *(file_counts + 1) = (*(arr_dims+1));
          *(file_counts + 2) = (*(arr_dims+2));
#endif
      } else {
          sprintf(error_string,"ADI array n_dims=%d rather than 1, 2, or 3",
                  (int)arr_n_dims);
          free(arr_dims);
          H5Sclose(dataspace);
          H5Tclose(read_datatype);
          H5Dclose(dataset);
          H5Gclose(adi_group);
          return(DM_FILEIO_FAILURE);
      }    
  
      if ((local_nx != ptr_adi_array_struct->nx) ||
          (local_ny != ptr_adi_array_struct->ny) ||
          (local_nz != ptr_adi_array_struct->nz)) {
          sprintf(error_string,"ADI array dimensions are [%d,%d,%d] not [%d,%d,%d]",
                  local_nx,local_ny,local_nz,
                  ptr_adi_array_struct->nx,ptr_adi_array_struct->ny,
                  ptr_adi_array_struct->nz);
          H5Tclose(read_datatype);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(adi_group);
          free(arr_dims);
#if USE_MPI
          free(file_offsets);
          free(file_counts);
#endif
          return(DM_FILEIO_FAILURE);
      }
  } /* endif(my_rank == 0) */

#if USE_MPI
  /* Distribute data onto nodes. npix will tell us the total number of
   * elements so we need to divide by the number of processes..
   */
  for (i = 0; i < p; i++) {
      if (my_rank == 0) {
          
          /* Determine the file_offsets here. Note that we have to
           * divide by number of processes.
           */
          if (arr_n_dims == 1) {
              file_offsets[0] = i*ptr_adi_array_struct->nx/p;
          } else if (arr_n_dims == 2) {
              file_offsets[0] = i*ptr_adi_array_struct->ny/p;
          } else if (arr_n_dims == 3) {
              file_offsets[0] = i*ptr_adi_array_struct->nz/p;
          }

          if ((memspace =
               H5Screate_simple(arr_n_dims,file_counts,NULL)) < 0) {
              sprintf(error_string,"H5Screate_simple(adi_array) error");
              H5Sclose(dataspace);
              H5Tclose(read_datatype);
              H5Dclose(dataset);
              H5Gclose(adi_group);
              free(file_offsets);
              free(file_counts);
              return(DM_FILEIO_FAILURE);
          }

          if ((status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                            file_offsets, NULL,
                                            file_counts, NULL)) < 0) {
              strcpy(error_string,
                     "Error in H5Sselect_hyperslab(adi_array)");
              H5Sclose(dataspace);
              H5Sclose(memspace);
              H5Tclose(read_datatype);
              H5Gclose(adi_group);
              free(file_offsets);
              free(file_counts);
              return(DM_FILEIO_FAILURE);
          }

          if (i == 0) {
              /* read directly into the adi_array_struct */
              if ((status = H5Dread(dataset,read_datatype,
                                    memspace,dataspace,H5P_DEFAULT,
                                    ptr_adi_array_struct->real_array)) < 0) {
                  strcpy(error_string, "Error in H5Dwrite (adi_array)");
                  H5Sclose(dataspace);
                  H5Sclose(memspace);
                  H5Tclose(datatype);
                  H5Gclose(adi_group);
                  free(file_offsets);
                  free(file_counts);
                  return(DM_FILEIO_FAILURE);
              }              
          } else {
              /* Read into slice array to send it */
              slice_array =
                  (dm_array_real *)malloc(sizeof(dm_array_real)*
                                          ptr_adi_array_struct->npix/p);
              
              if ((status = H5Dread(dataset,read_datatype,
                                    memspace,dataspace,H5P_DEFAULT,
                                    slice_array)) < 0) {
                  strcpy(error_string, "Error in H5Dwrite (adi_array)");
                  H5Sclose(dataspace);
                  H5Sclose(memspace);
                  H5Tclose(datatype);
                  H5Gclose(adi_group);
                  free(file_offsets);
                  free(file_counts);
                  free(slice_array);
                  return(DM_FILEIO_FAILURE);
              }
              
              MPI_Send(slice_array,ptr_adi_array_struct->npix/p,
                       MPI_ARRAY_REAL,i,99,MPI_COMM_WORLD);
              
              free(slice_array);
          } /* endif(i == 0) */
      }  /* endif(my_rank == 0) */

      if (i > 0) {
          if (my_rank == i) {
              
              MPI_Recv(ptr_adi_array_struct->real_array,
                       ptr_adi_array_struct->npix/p,
                       MPI_ARRAY_REAL, 0, 99,MPI_COMM_WORLD,&mpi_status);
          
          } /* endif(my_rank == i) */
      } /* endif(i > 0) */
  } /* endfor */
          
#else /* no USE_MPI */
  
  if ((memspace = H5Screate_simple(arr_n_dims,arr_dims,NULL)) < 0) {
      sprintf(error_string,"H5Screate_simple(adi_array) error");
      H5Sclose(dataspace);
      H5Tclose(read_datatype);
      H5Dclose(dataset);
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
  }
  
  if ((status = H5Dread(dataset,read_datatype,
			memspace,dataspace,H5P_DEFAULT,
			ptr_adi_array_struct->real_array)) < 0) {
      sprintf(error_string,"H5Dread(adi_array) error");
      H5Sclose(dataspace);
      H5Sclose(memspace);
      H5Tclose(read_datatype);
      H5Dclose(dataset);
      H5Gclose(adi_group);
      return(DM_FILEIO_FAILURE);
  }

#endif /* USE_MPI */
  if (my_rank == 0) {
      H5Sclose(dataspace);
      H5Tclose(read_datatype);
      H5Dclose(dataset);
      H5Sclose(memspace);
      free(arr_dims);
#if USE_MPI
      free(file_offsets);
      free(file_counts);
#endif
  } /* endif(my_rank == 0) */

  /* Check if the user wants to read out an error array.
   * In case there is no error array we'll set the npix to zero quietly.
   */
  if (my_rank == 0) {
      if (ptr_adi_error_array_struct->npix == 0) {
          error_exists = 0;
      } else {
          error_exists = 1;
      }
  } /* endif(my_rank == 0) */

  /* See if error array exists and let the other nodes know if we are
   * using MPI.
   */
#if USE_MPI
  MPI_Bcast(&error_exists,1,MPI_INT,0,MPI_COMM_WORLD);
#endif /* USE_MPI */

  /*--- adi_error_array ---*/
  if (error_exists == 0) {
      /* There's no need to even attempt reading an error array */
  } else if (error_exists == 1) {
      if (my_rank == 0) {
          if ((dataset = H5Dopen(adi_group,"adi_error_array")) < 0) {
              /* If there was no adi_error array, don't report an error; just
               * set the array size to zero. */
              ptr_adi_error_array_struct->nx = 0;
              ptr_adi_error_array_struct->ny = 0;
              ptr_adi_error_array_struct->nz = 0;
              ptr_adi_error_array_struct->npix = 0;
              H5Dclose(dataset);
              no_error = 1;
          } else {
              no_error = 0;
          }
      } /* endif(my_rank == 0) */

      /* Now broadcast this if we use MPI */
#if USE_MPI
      MPI_Bcast(&no_error,1,MPI_INT,0,MPI_COMM_WORLD);
#endif /* USE_MPI */

      /* Now if there is no error then we jump to the end. We have to do it
       * in this clumsy way to make sure that all other processes
       * besides root will also exit quietly if we use MPI.
       */
      if (no_error == 0) {
      
          if (my_rank == 0) {
              /* There is an adi_error_array, and we should read it */
              if ((datatype = H5Dget_type(dataset)) < 0) {
                  strcpy(error_string,"H5Dget_type(adi_error_array) error");
                  H5Dclose(dataset);
                  H5Gclose(adi_group);
                  return(DM_FILEIO_FAILURE);
              }
      
              /*--- get order of type ---*/
              if ((order = H5Tget_order(datatype)) < 0) {
                  strcpy(error_string,"H5Tget_order(adi_array) error");
                  H5Dclose(dataset);
                  H5Tclose(datatype);
                  H5Gclose(adi_group);
                  return(DM_FILEIO_FAILURE);
              }
      
              /*--- create local datatype to compare the orders ---*/
              if ((local_datatype = H5Tcopy(H5T_NATIVE_FLOAT)) < 0) {
                  strcpy(error_string,"H5Tcopy(adi_array) error");
                  H5Dclose(dataset);
                  H5Tclose(datatype);
                  H5Gclose(adi_group);
                  return(DM_FILEIO_FAILURE);
              }
      
              /*--- get order of local type ---*/
              if ((local_order = H5Tget_order(local_datatype)) < 0) {
                  strcpy(error_string,"H5Tget_order(adi_array) error");
                  H5Dclose(dataset);
                  H5Tclose(datatype);
                  H5Gclose(adi_group);
                  H5Tclose(local_datatype);
                  return(DM_FILEIO_FAILURE);
              }
      
              /*--- Convert to local order if necessary ---*/
              read_datatype = H5Tcopy(datatype);
              if (local_order != order) {
                  if ((status =
                       H5Tset_order(read_datatype,local_order)) < 0) {
                      strcpy(error_string,"H5Tset_order(adi_array) error");
                      H5Dclose(dataset);
                      H5Tclose(datatype);
                      H5Tclose(local_datatype);
                      H5Gclose(adi_group);
                      return(DM_FILEIO_FAILURE);
                  } 
              }
              H5Tclose(local_datatype);
              H5Tclose(datatype);
      
              if ((dataspace = H5Dget_space(dataset)) < 0) {
                  strcpy(error_string,"H5Dget_space(adi_error_array) error");
                  H5Sclose(dataspace);
                  H5Tclose(read_datatype);
                  H5Dclose(dataset);
                  H5Gclose(adi_group);
                  return(DM_FILEIO_FAILURE);
              }
              arr_n_dims = H5Sget_simple_extent_ndims(dataspace);
              arr_dims = (hsize_t *)malloc(arr_n_dims*sizeof(hsize_t));
              if ((status = H5Sget_simple_extent_dims(dataspace,arr_dims,
                                                      NULL)) < 0) {
                  strcpy(error_string,
                         "H5Dget_simple_extent_dims(adi_error_array) error");
                  free(arr_dims);
                  H5Sclose(dataspace);
                  H5Tclose(read_datatype);
                  H5Dclose(dataset);
                  H5Gclose(adi_group);
                  return(DM_FILEIO_FAILURE);
              }
      
              if (arr_n_dims == 1) {
                  local_nx = (*(arr_dims+0));
                  local_ny = 1;
                  local_nz = 1;
#if USE_MPI
                  file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
                  *(file_offsets + 0) = 0; /* Will be adjusted for each
                                              slice */
                  
                  /* Note that we have to divide by the number of
                   * processes here.
                   */
                  file_counts = malloc(arr_n_dims*sizeof(hsize_t));
                  *(file_counts + 0) = (*(arr_dims+0))/p;
#endif
              } else if (arr_n_dims == 2) {
                  local_ny = (*(arr_dims+0));
                  local_nx = (*(arr_dims+1));
                  local_nz = 1;
#if USE_MPI
                  file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
                  *(file_offsets + 0) = 0;
                  *(file_offsets + 1) = 0; /* Will be adjusted for each
                                              slice */
                  
                  /* Note that we have to divide by the number of
                   * processes here.
                   */
                  file_counts = malloc(arr_n_dims*sizeof(hsize_t));
                  *(file_counts + 0) = (*(arr_dims+0))/p;
                  *(file_counts + 1) = (*(arr_dims+1));
#endif
              } else if (arr_n_dims == 3) {
                  local_nz = (*(arr_dims+0));
                  local_ny = (*(arr_dims+1));
                  local_nx = (*(arr_dims+2));
#if USE_MPI
                  file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
                  *(file_offsets + 0) = 0;
                  *(file_offsets + 1) = 0;
                  *(file_offsets + 2) = 0; /* Will be adjusted for each
                                              slice */
                  
                  /* Note that we have to divide by the number of
                   * processes here.
                   */
                  file_counts = malloc(arr_n_dims*sizeof(hsize_t));
                  *(file_counts + 0) = (*(arr_dims+0))/p;
                  *(file_counts + 1) = (*(arr_dims+1));
                  *(file_counts + 2) = (*(arr_dims+2));
#endif
              } else {
                  sprintf(error_string,"ADI array n_dims=%d rather than 2 or 3",
                          (int)arr_n_dims);
                  free(arr_dims);
                  H5Tclose(read_datatype);
                  H5Dclose(dataset);
                  H5Gclose(adi_group);
                  return(DM_FILEIO_FAILURE);
              }    

              if ((local_nx != ptr_adi_error_array_struct->nx) ||
                  (local_ny != ptr_adi_error_array_struct->ny) ||
                  (local_nz != ptr_adi_error_array_struct->nz)) {
                  sprintf(error_string,
                          "adi_error_array dimensions are [%d,%d,%d] not [%d,%d,%d]",
                          local_nx,local_ny,local_nz,
                          ptr_adi_error_array_struct->nx,
                          ptr_adi_error_array_struct->ny,
                          ptr_adi_error_array_struct->nz);
                  H5Tclose(read_datatype);
                  H5Dclose(dataset);
                  H5Gclose(adi_group);
                  free(arr_dims);
#if USE_MPI
                  free(file_offsets);
                  free(file_counts);
#endif
                  return(DM_FILEIO_FAILURE);
              }
          } /* endif(my_rank == 0) */

#if USE_MPI
          /* Distribute data onto nodes. npix will tell us the total
           * number of elements so we need to divide by the number
           * of processes...
           */
          for (i = 0; i < p; i++) {
              if (my_rank == 0) {
                  /* Determine the file_offsets here. Note that we have to
                   * divide by number of processes.
                   */
                  if (arr_n_dims == 1) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->nx/p;
                  } else if (arr_n_dims == 2) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->ny/p;
                  } else if (arr_n_dims == 3) {
                      file_offsets[0] = i*ptr_adi_error_array_struct->nz/p;
                  }
                  
                  if ((memspace =
                       H5Screate_simple(arr_n_dims,file_counts,NULL)) < 0) {
                      sprintf(error_string,
                              "H5Screate_simple(adi_array) error");
                      H5Sclose(dataspace);
                      H5Tclose(read_datatype);
                      H5Dclose(dataset);
                      H5Gclose(adi_group);
                      free(file_offsets);
                      free(file_counts);
                      free(slice_array);
                      return(DM_FILEIO_FAILURE);
                  }
                  
                  if ((status =
                       H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                           file_offsets, NULL,
                                           file_counts, NULL)) < 0) {
                      strcpy(error_string,
                             "Error in H5Sselect_hyperslab(adi_array)");
                      H5Sclose(dataspace);
                      H5Sclose(memspace);
                      H5Tclose(read_datatype);
                      H5Gclose(adi_group);
                      free(file_offsets);
                      free(file_counts);
                      free(slice_array);
                      return(DM_FILEIO_FAILURE);
                  }
                  if (i == 0) {
                      /* Read directly into adi_error_array_struct */
                      if ((status =
                           H5Dread(dataset,read_datatype,
                                   memspace,dataspace,H5P_DEFAULT,
                                   ptr_adi_error_array_struct->real_array)) < 0) {
                          strcpy(error_string, "Error in H5Dread (adi_array)");
                          H5Sclose(dataspace);
                          H5Sclose(memspace);
                          H5Tclose(datatype);
                          H5Gclose(adi_group);
                          free(file_offsets);
                          free(file_counts);
                          return(DM_FILEIO_FAILURE);
                      }
                  } else {
                      /* Read into slice array to send around */
                      slice_array =(dm_array_real *)malloc(sizeof(dm_array_real)*
                                                           ptr_adi_error_array_struct->npix/p);
                  

                      if ((status = H5Dread(dataset,read_datatype,
                                            memspace,dataspace,H5P_DEFAULT,
                                            slice_array)) < 0) {
                          strcpy(error_string, "Error in H5Dread (adi_array)");
                          H5Sclose(dataspace);
                          H5Sclose(memspace);
                          H5Tclose(datatype);
                          H5Gclose(adi_group);
                          free(file_offsets);
                          free(file_counts);
                          free(slice_array);
                          return(DM_FILEIO_FAILURE);
                      }
                  
                      MPI_Send(slice_array,ptr_adi_error_array_struct->npix/p,
                               MPI_ARRAY_REAL,i,99,MPI_COMM_WORLD);
                      
                      free(slice_array);
                  } /* endif(i == 0) */
              }  /* endif(my_rank == 0) */ 

              if (i > 0) {
                  if (my_rank == i) {                  
                      MPI_Recv(ptr_adi_error_array_struct->real_array,
                               ptr_adi_error_array_struct->npix/p,
                               MPI_ARRAY_REAL, 0, 99,MPI_COMM_WORLD,&mpi_status);
                      
                      
                  } /* endif(my_rank == i) */
              } /* endif(i > 0) */
          } /* endfor */
          
#else /* no USE_MPI */
          
          if ((memspace = H5Screate_simple(arr_n_dims,arr_dims,NULL)) < 0) {
              sprintf(error_string,"H5Screate_simple(adi_array) error");
              H5Sclose(dataspace);
              H5Tclose(read_datatype);
              H5Dclose(dataset);
              H5Gclose(adi_group);
              return(DM_FILEIO_FAILURE);
          }
          
          if ((status =
               H5Dread(dataset,read_datatype,memspace,dataspace,
                       H5P_DEFAULT,
                       ptr_adi_error_array_struct->real_array)) < 0) {
              sprintf(error_string,"H5Dread(adi_error_array) error");
              H5Sclose(dataspace);
              H5Sclose(memspace);
              H5Tclose(read_datatype);
              H5Dclose(dataset);
              H5Gclose(adi_group);
              return(DM_FILEIO_FAILURE);
          }
#endif /* USE_MPI */
          
          if (my_rank == 0) {
              H5Sclose(dataspace);
              H5Tclose(read_datatype);
              H5Dclose(dataset);
              H5Sclose(memspace);
              free(arr_dims);
#if USE_MPI
              free(file_offsets);
              free(file_counts);
#endif
          } /* endif(my_rank == 0) */
          
      } /* endif(no_error == 0) */
  } /* endif(error_exists == 1) */

  if (my_rank == 0) {
      H5Gclose(adi_group);
  } /* endif(my_rank == 0) */
  
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_spt_info(hid_t h5_file_id,
			int *ptr_nx, int *ptr_ny, int *ptr_nz,
			dm_spt_struct *ptr_spt_struct,
			char *error_string,
                        int my_rank)
{
  hid_t spt_group;
  hid_t datatype, dataspace, dataset;
  hid_t attr;
  hsize_t *arr_dims;
  herr_t status;                             
  int arr_n_dims, dm_spt_version;

  strcpy(error_string,"");

  if (my_rank == 0) {

      if ((spt_group = H5Gopen(h5_file_id,"/spt")) < 0) {
          strcpy(error_string,"H5Gopen(\"/spt\") error");
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }

      if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(datatype) error");
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Tset_size(datatype,H5_SIZEOF_INT);
      if ((attr = H5Aopen_name(spt_group,"spt_version")) < 0) {
          strcpy(error_string,"H5Aopen_name(spt_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Aread(attr,datatype,
                            &dm_spt_version)) < 0) {
          strcpy(error_string,"H5Aread(spt_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Aclose(attr);
      H5Tclose(datatype);
  
      if (dm_spt_version > DM_SPT_VERSION) {
          sprintf(error_string,
                  "Can only handle SPT version up to %d, not %d",
                  DM_SPT_VERSION,dm_spt_version);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
  
      /*--- check on dimensions of spt_array ---*/
      if ((dataset = H5Dopen(spt_group,"spt_array")) < 0) {
          strcpy(error_string,"H5Dopen(\"spt_array\") error");
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }

      if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Dget_space(spt_array) error");
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      arr_n_dims = H5Sget_simple_extent_ndims(dataspace);
      arr_dims = (hsize_t *)malloc(arr_n_dims*sizeof(hsize_t));
      if ((status = H5Sget_simple_extent_dims(dataspace,arr_dims,NULL)) < 0) {
          strcpy(error_string,"H5Dget_simple_extent_dims(spt_array) error");
          free(arr_dims);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }

      if (arr_n_dims == 1) {
          *ptr_nx = (*(arr_dims+0));
          *ptr_ny = 1;
          *ptr_nz = 1;
      } else if (arr_n_dims == 2) {
          *ptr_ny = (*(arr_dims+0));
          *ptr_nx = (*(arr_dims+1));
          *ptr_nz = 1;
      } else if (arr_n_dims == 3) {
          *ptr_nz = (*(arr_dims+0));
          *ptr_ny = (*(arr_dims+1));
          *ptr_nx = (*(arr_dims+2));
      } else {
          sprintf(error_string,
		  "Error: SPT array n_dims=%d rather than 1, 2, or 3",
                  (int)arr_n_dims);
          free(arr_dims);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }    
      free(arr_dims);
      H5Sclose(dataspace);
      H5Dclose(dataset);

  } /* endif(my_rank == 0) */

    /* Send info on array size to other processes if necesary */
#if USE_MPI
  MPI_Bcast(ptr_nx,1,MPI_INT,0,MPI_COMM_WORLD);
  MPI_Bcast(ptr_ny,1,MPI_INT,0,MPI_COMM_WORLD);
  MPI_Bcast(ptr_nz,1,MPI_INT,0,MPI_COMM_WORLD);
#endif /* USE_MPI */

  /*----------------------------------------------------------------*/
  /* Now get spt_struct */
  if (my_rank == 0) {
      if ((dataset = H5Dopen(spt_group,"spt_struct")) < 0) {
          strcpy(error_string,"H5Dopen(spt_struct) error");
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Gget_space(spt_struct) error");
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((datatype = H5Tcreate(H5T_COMPOUND,
                                sizeof(dm_spt_struct))) < 0) {
          strcpy(error_string,"H5Tcreate(spt_struct) error");
          H5Tclose(datatype);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      dm_h5_insert_spt_struct_members(datatype);
      
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            ptr_spt_struct)) < 0) {
          strcpy(error_string,"H5Dread(spt_struct) error");
          H5Tclose(datatype);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Tclose(datatype);
      H5Sclose(dataspace);
      H5Dclose(dataset);
      H5Gclose(spt_group);
      
  } /* endif(my_rank == 0) */
  
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_spt(hid_t h5_file_id,
		   dm_array_byte_struct *ptr_spt_array_struct,
		   char *error_string,
                   int my_rank,
                   int p)
{
  hid_t spt_group;
  hid_t datatype, dataspace, dataset;
  hid_t mem_type_id;
  hid_t attr;
  hsize_t *arr_dims;
  herr_t status;                             
  int arr_n_dims, dm_spt_version;
  int local_nx, local_ny, local_nz;
#if USE_MPI
  hsize_t *file_offsets, *file_counts;
  hid_t memspace;
  int i;
  dm_array_real *slice_array;
  MPI_Status mpi_status;
#endif

  if (my_rank == 0) {
      strcpy(error_string,"");
      
      if ((spt_group = H5Gopen(h5_file_id,"/spt")) < 0) {
          strcpy(error_string,"H5Gopen(\"/spt\") error");
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
  
      if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(datatype) error");
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Tset_size(datatype,H5_SIZEOF_INT);
      if ((attr = H5Aopen_name(spt_group,"spt_version")) < 0) {
          strcpy(error_string,"H5Aopen_name(spt_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Aread(attr,datatype,
                            &dm_spt_version)) < 0) {
          strcpy(error_string,"H5Aread(comment_string_length) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Aclose(attr);
      H5Tclose(datatype);
  
      if (dm_spt_version > DM_SPT_VERSION) {
          sprintf(error_string,
                  "Can only handle SPT version up to %d, not %d",
                  DM_SPT_VERSION,dm_spt_version);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
  
      /*--- spt_array ---*/
      if ((dataset = H5Dopen(spt_group,"spt_array")) < 0) {
          strcpy(error_string,"H5Dopen(\"spt_array\") error");
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((datatype = H5Dget_type(dataset)) < 0) {
          strcpy(error_string,"H5Dget_type(spt_array) error");
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Dget_space(spt_array) error");
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
      arr_n_dims = H5Sget_simple_extent_ndims(dataspace);
      arr_dims = (hsize_t *)malloc(arr_n_dims*sizeof(hsize_t));
      if ((status = H5Sget_simple_extent_dims(dataspace,arr_dims,NULL)) < 0) {
          strcpy(error_string,"H5Dget_simple_extent_dims(spt_array) error");
          free(arr_dims);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }
    
      if (arr_n_dims == 1) {
          local_nx = (*(arr_dims+0));
          local_ny = 1;
          local_nz = 1;
#if USE_MPI
          file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_offsets + 0) = 0; /* Will be adjusted for each slice */
          
          /* Note that we have to divide by the number of processes here.
           */
          file_counts = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_counts + 0) = (*(arr_dims+0))/p;
#endif
      } else if (arr_n_dims == 2) {
          local_ny = (*(arr_dims+0));
          local_nx = (*(arr_dims+1));
          local_nz = 1;
#if USE_MPI
          file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_offsets + 0) = 0;
          *(file_offsets + 1) = 0; /* Will be adjusted for each slice */
          
          /* Note that we have to divide by the number of processes here.
           */
          file_counts = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_counts + 0) = (*(arr_dims+0))/p;
          *(file_counts + 1) = (*(arr_dims+1));
#endif
      } else if (arr_n_dims == 3) {
          local_nz = (*(arr_dims+0));
          local_ny = (*(arr_dims+1));
          local_nx = (*(arr_dims+2));
#if USE_MPI
          file_offsets = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_offsets + 0) = 0;
          *(file_offsets + 1) = 0;
          *(file_offsets + 2) = 0; /* Will be adjusted for each slice */
           
          /* Note that we have to divide by the number of processes here.
           */
          file_counts = malloc(arr_n_dims*sizeof(hsize_t));
          *(file_counts + 0) = (*(arr_dims+0))/p;
          *(file_counts + 1) = (*(arr_dims+1));
          *(file_counts + 2) = (*(arr_dims+2));
#endif
      } else {
          sprintf(error_string,"SPT array n_dims=%d rather than 1, 2, or 3",
                  (int)arr_n_dims);
          free(arr_dims);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(spt_group);
          return(DM_FILEIO_FAILURE);
      }    
      free(arr_dims);
  
      if ((local_nx != ptr_spt_array_struct->nx) ||
          (local_ny != ptr_spt_array_struct->ny) ||
          (local_nz != ptr_spt_array_struct->nz)) {
          sprintf(error_string,"SPT array dimensions are [%d,%d,%d] not [%d,%d,%d]",
                  local_nx,local_ny,local_nz,
                  ptr_spt_array_struct->nx,ptr_spt_array_struct->ny,
                  ptr_spt_array_struct->nz);
          H5Sclose(dataspace);
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(spt_group);
#if USE_MPI
          free(file_offsets);
          free(file_counts);
#endif
          return(DM_FILEIO_FAILURE);
      }
  
      mem_type_id = H5Tcopy(H5T_NATIVE_UINT8);

  } /* endif(my_rank == 0) */

  #if USE_MPI
  /* Distribute data onto nodes. npix will tell us the total number of
   * elements so we need to divide by the number of processes..
   */
  for (i = 0; i < p; i++) {
      if (my_rank == 0) {
          
          /* Determine the file_offsets here. Note that we have to
           * divide by number of processes.
           */
          if (arr_n_dims == 1) {
              file_offsets[0] = i*ptr_spt_array_struct->nx/p;
          } else if (arr_n_dims == 2) {
              file_offsets[0] = i*ptr_spt_array_struct->ny/p;
          } else if (arr_n_dims == 3) {
              file_offsets[0] = i*ptr_spt_array_struct->nz/p;
          }

          if ((memspace =
               H5Screate_simple(arr_n_dims,file_counts,NULL)) < 0) {
              sprintf(error_string,"H5Screate_simple(adi_array) error");
              H5Sclose(dataspace);
              H5Tclose(mem_type_id);
              H5Dclose(dataset);
              H5Gclose(spt_group);
              free(file_offsets);
              free(file_counts);
              free(slice_array);
              return(DM_FILEIO_FAILURE);
          }

          if ((status = H5Sselect_hyperslab(dataspace, H5S_SELECT_SET,
                                            file_offsets, NULL,
                                            file_counts, NULL)) < 0) {
              strcpy(error_string,
                     "Error in H5Sselect_hyperslab(adi_array)");
              H5Sclose(dataspace);
              H5Sclose(memspace);
              H5Tclose(mem_type_id);
              H5Gclose(spt_group);
              free(file_offsets);
              free(file_counts);
              free(slice_array);
              return(DM_FILEIO_FAILURE);
          }

          if (i == 0) {
              /* read directly into spt_array_struct */
              if ((status = H5Dread(dataset,mem_type_id,
                                    memspace,dataspace,H5P_DEFAULT,
                                    ptr_spt_array_struct->byte_array)) < 0) {
                  strcpy(error_string, "Error in H5Dwrite (adi_array)");
                  H5Sclose(dataspace);
                  H5Sclose(memspace);
                  H5Tclose(mem_type_id);
                  H5Gclose(spt_group);
                  free(file_offsets);
                  free(file_counts);
                  return(DM_FILEIO_FAILURE);
              }

          } else {
              slice_array =
                  (dm_array_real *)malloc(sizeof(dm_array_real)*
                                          ptr_spt_array_struct->npix/p);

              if ((status = H5Dread(dataset,mem_type_id,
                                    memspace,dataspace,H5P_DEFAULT,
                                    slice_array)) < 0) {
                  strcpy(error_string, "Error in H5Dwrite (adi_array)");
                  H5Sclose(dataspace);
                  H5Sclose(memspace);
                  H5Tclose(mem_type_id);
                  H5Gclose(spt_group);
                  free(file_offsets);
                  free(file_counts);
                  free(slice_array);
                  return(DM_FILEIO_FAILURE);
              }
              
              MPI_Send(slice_array,ptr_spt_array_struct->npix/p,
                       MPI_BYTE,i,99,MPI_COMM_WORLD);
              
              free(slice_array);
          } /* endif(i == 0) */
      }  /* endif(my_rank == 0) */
            
      if (i > 0) {
          if (my_rank == i) {
              
              MPI_Recv(ptr_spt_array_struct->byte_array,
                       ptr_spt_array_struct->npix/p,
                       MPI_BYTE, 0, 99,MPI_COMM_WORLD,&mpi_status);
              
          } /* endif(my_rank == i) */
      } /* endif(i > 0) */
  } /* endfor */
          
#else /* no USE_MPI */  
  if ((status = H5Dread(dataset,mem_type_id,
			H5S_ALL,H5S_ALL,H5P_DEFAULT,
			ptr_spt_array_struct->byte_array)) < 0) {
      sprintf(error_string,"H5Dread(spt_array) error");
      H5Sclose(dataspace);
      H5Tclose(datatype);
      H5Dclose(dataset);
      H5Gclose(spt_group);
      return(DM_FILEIO_FAILURE);
  }

#endif /* USE_MPI */
  if (my_rank == 0) {
      H5Sclose(dataspace);
      H5Tclose(datatype);
      H5Dclose(dataset);
#if USE_MPI
      H5Sclose(memspace);
      free(file_offsets);
      free(file_counts);
#endif
  } /* endif(my_rank == 0) */

  if (my_rank == 0) {
      H5Gclose(spt_group);
  } /* endif(my_rank == 0) */

  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_itn_info(hid_t h5_file_id,
			int *ptr_nx, int *ptr_ny, int *ptr_nz,
			int *ptr_recon_errors_npix,
			dm_itn_struct *ptr_itn_struct,
			char *error_string,
                        int my_rank)
{
  hid_t itn_group;
  hid_t datatype, dataset, dataspace;
  hid_t attr;
  hsize_t *file_dims;
  herr_t status;                             
  int file_n_dims, dm_itn_version, n_complex,i;

  strcpy(error_string,"");

  if (my_rank == 0) {
      if ((itn_group = H5Gopen(h5_file_id,"/itn")) < 0) {
          strcpy(error_string,"H5Gopen(\"/itn\") error");
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }

      if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(datatype) error");
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Tset_size(datatype,H5_SIZEOF_INT);
      if ((attr = H5Aopen_name(itn_group,"itn_version")) < 0) {
          strcpy(error_string,"H5Aopen_name(itn_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Aread(attr,datatype,
                            &dm_itn_version)) < 0) {
          strcpy(error_string,"H5Aread(itn_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Aclose(attr);
      H5Tclose(datatype);
  
      if (dm_itn_version > DM_ITN_VERSION) {
          sprintf(error_string,
                  "Can only handle ITN version up to %d, not %d",
                  DM_ITN_VERSION,dm_itn_version);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
  
      /*--- check on dimensions of itn_array ---*/
      if ((dataset = H5Dopen(itn_group,"itn_array")) < 0) {
          strcpy(error_string,"H5Dopen(\"itn_array\") error");
          H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }

      if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Dget_space(itn_array) error");
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      file_n_dims = H5Sget_simple_extent_ndims(dataspace);
      file_dims = (hsize_t *)malloc(file_n_dims*sizeof(hsize_t));
      if ((status = H5Sget_simple_extent_dims(dataspace,file_dims,NULL)) < 0) {
          strcpy(error_string,"H5Dget_simple_extent_dims(itn_array) error");
          free(file_dims);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }

      if (file_n_dims == 2) {
          *ptr_nx = (*(file_dims+0));
          n_complex = (*(file_dims+1));
          *ptr_ny = 1;
          *ptr_nz = 1;
      } else if (file_n_dims == 3) {
          *ptr_ny = (*(file_dims+0));
          *ptr_nx = (*(file_dims+1));
          n_complex = (*(file_dims+2));
          *ptr_nz = 1;
      } else if (file_n_dims == 4) {
          *ptr_nz = (*(file_dims+0));
          *ptr_ny = (*(file_dims+1));
          *ptr_nx = (*(file_dims+2));
          n_complex = (*(file_dims+3));
      } else {
          sprintf(error_string,
                  "Error: ITN array n_dims=%d rather than 2, 3, or 4 for complex",
                  (int)file_n_dims);
          free(file_dims);
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }    
      free(file_dims);

      if (n_complex != 2) {
          sprintf(error_string,
                  "Error: ITN array should have fast index=2 not %d for complex",
                  (int)n_complex);
          H5Sclose(dataspace);
	  H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Sclose(dataspace);
      H5Dclose(dataset);
      

      /*--- check on dimensions of recon_errors ---*/
      if ((dataset = H5Dopen(itn_group,"recon_errors")) < 0) {
	/* It does not exist, in this case simply return 0 */
	*ptr_recon_errors_npix = -1;

      } else {
	if ((dataspace = H5Dget_space(dataset)) < 0) {
          strcpy(error_string,"H5Dget_space(itn_array) error");
          H5Sclose(dataspace);
          H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
	}

	file_n_dims = 1;
	file_dims = (hsize_t *)malloc(file_n_dims*sizeof(hsize_t));
	if ((status = 
	     H5Sget_simple_extent_dims(dataspace,file_dims,NULL)) < 0) {
          strcpy(error_string,"H5Dget_simple_extent_dims(recon_errors) error");
          free(file_dims);
          H5Sclose(dataspace);
	  H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
	}
	*ptr_recon_errors_npix = *(file_dims + 0);

	free(file_dims);
      }
  } /* endif(my_rank == 0) */
  
  /* Send info on array size to other processes if necesary */
#if USE_MPI
  MPI_Bcast(ptr_nx,1,MPI_INT,0,MPI_COMM_WORLD);
  MPI_Bcast(ptr_ny,1,MPI_INT,0,MPI_COMM_WORLD);
  MPI_Bcast(ptr_nz,1,MPI_INT,0,MPI_COMM_WORLD);
#endif /* USE_MPI */
  
  /*----------------------------------------------------------------*/
  /* Now get itn_struct */
  if (my_rank == 0) {
      if ((dataset = H5Dopen(itn_group,"itn_struct")) < 0) {
          strcpy(error_string,"H5Dopen(itn_struct) error");
          H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }

      if ((datatype = H5Tcreate(H5T_COMPOUND,
                                sizeof(dm_itn_struct))) < 0) {
          strcpy(error_string,"H5Tcreate(itn_struct) error");
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      dm_h5_insert_itn_struct_members(datatype);
  
      if ((status = H5Dread(dataset,datatype,
                            H5S_ALL,H5S_ALL,H5P_DEFAULT,
                            ptr_itn_struct)) < 0) {
          strcpy(error_string,"H5Dread(itn_struct) error");
          H5Tclose(datatype);
          H5Dclose(dataset);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Tclose(datatype);
      H5Dclose(dataset);
      H5Gclose(itn_group);

  } /* endif(my_rank == 0) */
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_h5_read_itn(hid_t h5_file_id,
		   dm_array_real_struct *ptr_recon_errors,
		   dm_array_complex_struct *ptr_itn_array_struct,
		   char *error_string,
                   int my_rank,
                   int p)
{
  hid_t itn_group;
  hid_t datatype, dataset;
  hid_t file_dataspace, memory_dataspace;
  hid_t mem_type_id, local_datatype, read_datatype;
  hsize_t memory_dims[4], file_offsets[4], file_counts[4];
  hid_t attr;
  hsize_t *file_dims;
  herr_t status;
  int file_n_dims, dm_itn_version, n_complex;
  int local_nx, local_ny, local_nz;
  dm_array_index_t iz, zoffset, ipix, slice_npix;
  dm_array_real *slice_array;
  H5T_order_t order, local_order;
#if USE_MPI
  int i;
  MPI_Status mpi_status;
#endif

  strcpy(error_string,"");
  
  if (my_rank == 0) {
      if ((itn_group = H5Gopen(h5_file_id,"/itn")) < 0) {
          strcpy(error_string,"H5Gopen(\"/itn\") error");
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      
      if ((datatype = H5Tcopy(H5T_NATIVE_INT)) < 0) {
          strcpy(error_string,"H5Tcopy(datatype) error");
          H5Tclose(datatype);
          return(DM_FILEIO_FAILURE);
      }
      H5Tset_size(datatype,H5_SIZEOF_INT);
      if ((attr = H5Aopen_name(itn_group,"itn_version")) < 0) {
          strcpy(error_string,"H5Aopen_name(itn_version) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      if ((status = H5Aread(attr,datatype,
                            &dm_itn_version)) < 0) {
          strcpy(error_string,"H5Aread(comment_string_length) error");
          H5Aclose(attr);
          H5Tclose(datatype);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      H5Aclose(attr);
      H5Tclose(datatype);
      
      if (dm_itn_version > DM_ITN_VERSION) {
          sprintf(error_string,
                  "Can only handle ITN version up to %d, not %d",
                  DM_ITN_VERSION,dm_itn_version);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
      
      /* recon_errors */
      if ((dataset = H5Dopen(itn_group,"recon_errors")) < 0) {
	/* If there was no recon_errors, don't report an error; just
	 * set the array size to zero. */
	ptr_recon_errors->nx = 0;
	ptr_recon_errors->ny = 0;
	ptr_recon_errors->nz = 0;
	ptr_recon_errors->npix = 0;
	H5Dclose(dataset);
      } else {
	if ((datatype = H5Dget_type(dataset)) < 0) {
	  strcpy(error_string,"H5Dget_type(recon_errors) error");
	  H5Dclose(dataset);
	  H5Gclose(itn_group);
	  return(DM_FILEIO_FAILURE);
	}
	
	/*--- get order of type ---*/
	if ((order = H5Tget_order(datatype)) < 0) {
	  strcpy(error_string,"H5Tget_order(recon_errors) error");
	  H5Dclose(dataset);
	  H5Tclose(datatype);
	  H5Gclose(itn_group);
	  return(DM_FILEIO_FAILURE);
	}
      
	/*--- create local datatype to compare the orders ---*/
	if ((local_datatype = H5Tcopy(H5T_NATIVE_FLOAT)) < 0) {
	  strcpy(error_string,"H5Tcopy(recon_errors) error");
	  H5Dclose(dataset);
	  H5Tclose(datatype);
	  H5Gclose(itn_group);
	  return(DM_FILEIO_FAILURE);
	}
	
	/*--- get order of local type ---*/
	if ((local_order = H5Tget_order(local_datatype)) < 0) {
	  strcpy(error_string,"H5Tget_order(recon_errors) error");
	  H5Dclose(dataset);
	  H5Tclose(datatype);
	  H5Gclose(itn_group);
	  H5Tclose(local_datatype);
          return(DM_FILEIO_FAILURE);
	}
	
	/*--- Convert to local order if necessary ---*/
	read_datatype = H5Tcopy(datatype);
	if (local_order != order) {
	  if ((status = H5Tset_order(read_datatype,local_order)) < 0) {
	    strcpy(error_string,"H5Tset_order(recon_errors) error");
	    H5Dclose(dataset);
	    H5Tclose(datatype);
	    H5Tclose(local_datatype);
	    H5Gclose(itn_group);
	    return(DM_FILEIO_FAILURE);
	  } 
	}
	
	if ((status =
	     H5Dread(dataset,read_datatype,H5S_ALL,H5S_ALL,H5P_DEFAULT,
		     ptr_recon_errors->real_array)) < 0) {
	  strcpy(error_string, "Error in H5Dread (recon_errors)");
	  H5Tclose(datatype);
	  H5Gclose(itn_group);
	  H5Tclose(local_datatype);
	  H5Tclose(read_datatype);
	  H5Dclose(dataset);
	  return(DM_FILEIO_FAILURE);
	}
	
	H5Tclose(local_datatype);
	H5Tclose(datatype);
	H5Tclose(read_datatype);
	H5Dclose(dataset);
      }
      
      /*--- itn_array ---*/
      if ((dataset = H5Dopen(itn_group,"itn_array")) < 0) {
	strcpy(error_string,"H5Dopen(\"itn_array\") error");
	H5Dclose(dataset);
	H5Gclose(itn_group);
	return(DM_FILEIO_FAILURE);
      }
      if ((datatype = H5Dget_type(dataset)) < 0) {
	strcpy(error_string,"H5Dget_type(itn_array) error");
	H5Tclose(datatype);
	H5Dclose(dataset);
	H5Gclose(itn_group);
	return(DM_FILEIO_FAILURE);
      }
      
      /*--- get order of type ---*/
      if ((order = H5Tget_order(datatype)) < 0) {
	strcpy(error_string,"H5Tget_order(itn_array) error");
	H5Dclose(dataset);
	H5Tclose(datatype);
	H5Gclose(itn_group);
	return(DM_FILEIO_FAILURE);
      }
      
      /*--- create local datatype to compare the orders ---*/
      if ((local_datatype = H5Tcopy(H5T_NATIVE_FLOAT)) < 0) {
	strcpy(error_string,"H5Tcopy(itn_array) error");
	H5Dclose(dataset);
	H5Tclose(datatype);
	H5Gclose(itn_group);
	return(DM_FILEIO_FAILURE);
      }
      
      /*--- get order of local type ---*/
      if ((local_order = H5Tget_order(local_datatype)) < 0) {
	strcpy(error_string,"H5Tget_order(itn_array) error");
	H5Dclose(dataset);
	H5Tclose(datatype);
	H5Gclose(itn_group);
	H5Tclose(local_datatype);
	return(DM_FILEIO_FAILURE);
      }
      
      /*--- Convert to local order if necessary ---*/
      read_datatype = H5Tcopy(datatype);
      if (local_order != order) {
	if ((status = H5Tset_order(read_datatype,local_order)) < 0) {
	  strcpy(error_string,"H5Tset_order(itn_array) error");
	  H5Dclose(dataset);
	  H5Tclose(datatype);
	  H5Tclose(local_datatype);
	  H5Gclose(itn_group);
	  return(DM_FILEIO_FAILURE);
	} 
      }
      H5Tclose(local_datatype);
      H5Tclose(datatype);
      
      if ((file_dataspace = H5Dget_space(dataset)) < 0) {
	strcpy(error_string,"H5Dget_space(itn_array) error");
	H5Sclose(file_dataspace);
	H5Tclose(read_datatype);
	H5Dclose(dataset);
	H5Gclose(itn_group);
	return(DM_FILEIO_FAILURE);
      }
      file_n_dims = H5Sget_simple_extent_ndims(file_dataspace);
      file_dims = (hsize_t *)malloc(file_n_dims*sizeof(hsize_t));
      if ((status =
           H5Sget_simple_extent_dims(file_dataspace,file_dims,NULL)) < 0) {
	strcpy(error_string,"H5Dget_simple_extent_dims(itn_array) error");
	free(file_dims);
	H5Sclose(file_dataspace);
	H5Tclose(read_datatype);
	H5Dclose(dataset);
	H5Gclose(itn_group);
	return(DM_FILEIO_FAILURE);
      }
      
      if (file_n_dims == 2) {
	local_nx = (*(file_dims+0));
	n_complex = (*(file_dims+1));
	local_ny = 1;
	local_nz = 1;
      } else if (file_n_dims == 3) {
	local_ny = (*(file_dims+0));
	local_nx = (*(file_dims+1));
	n_complex = (*(file_dims+2));
	local_nz = 1;
      } else if (file_n_dims == 4) {
	local_nz = (*(file_dims+0));
	local_ny = (*(file_dims+1));
	local_nx = (*(file_dims+2));
	n_complex = (*(file_dims+3));
      } else {
	sprintf(error_string,
		"Error: ITN array n_dims=%d rather than 2, 3, or 4 for complex",
		(int)file_n_dims);
	free(file_dims);
	H5Sclose(file_dataspace);
	H5Tclose(read_datatype);
	H5Dclose(dataset);
	H5Gclose(itn_group);
	return(DM_FILEIO_FAILURE);
      }    
      
      if (n_complex != 2) {
	sprintf(error_string,
		"Error: ITN array should have fast index=2 not %d for complex",
		(int)file_n_dims);
	free(file_dims);
	H5Sclose(file_dataspace);
	H5Tclose(read_datatype);
	H5Dclose(dataset);
	H5Gclose(itn_group);
	return(DM_FILEIO_FAILURE);
      }    
      
      if ((local_nx != ptr_itn_array_struct->nx) ||
          (local_ny != ptr_itn_array_struct->ny) ||
          (local_nz != ptr_itn_array_struct->nz)) {
	sprintf(error_string,"ITN array dimensions are [%d,%d,%d] not [%d,%d,%d]",
		local_nx,local_ny,local_nz,
		ptr_itn_array_struct->nx,ptr_itn_array_struct->ny,
		ptr_itn_array_struct->nz);
	H5Sclose(file_dataspace);
	H5Tclose(read_datatype);
	H5Dclose(dataset);
	H5Gclose(itn_group);
	free(file_dims);
	return(DM_FILEIO_FAILURE);
      }
      
      /* Prepare for hyperslabs of the data.  "slice_npix" does not
       * reflect the factor of 2 for complex numbers; this is deliberate.
       */
      if (ptr_itn_array_struct->ny == 1) {
          file_offsets[0] = 0;
          file_offsets[1] = 0;
#if USE_MPI
          memory_dims[0] = file_dims[0]/p;
          memory_dims[1] = file_dims[1];
          file_counts[0] = file_dims[0]/p;
          file_counts[1] = file_dims[1];
          slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
          memory_dims[0] = file_dims[0];
          memory_dims[1] = file_dims[1];
          file_counts[0] = memory_dims[0];
          file_counts[1] = memory_dims[1];
          slice_npix = ptr_itn_array_struct->npix;
#endif /* USE_MPI */
      } else if (ptr_itn_array_struct->nz == 1) {
          file_offsets[0] = 0;
          file_offsets[1] = 0;
          file_offsets[2] = 0;
#if USE_MPI
          memory_dims[0] = file_dims[0]/p;
          memory_dims[1] = file_dims[1];
          memory_dims[2] = file_dims[2];
          file_counts[0] = file_dims[0]/p;
          file_counts[1] = file_dims[1];
          file_counts[2] = file_dims[2];
          slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
          memory_dims[0] = file_dims[0];
          memory_dims[1] = file_dims[1];
          memory_dims[2] = file_dims[2];
          file_counts[0] = memory_dims[0];
          file_counts[1] = memory_dims[1];
          file_counts[2] = memory_dims[2];
          slice_npix = ptr_itn_array_struct->npix;
#endif /* USE_MPI */
      } else {
          file_offsets[0] = 0; /* Will be adjusted for each slice */
          file_offsets[1] = 0;
          file_offsets[2] = 0;
          file_offsets[3] = 0;
#if USE_MPI
          memory_dims[0] = file_dims[0]/p;
          memory_dims[1] = file_dims[1];
          memory_dims[2] = file_dims[2];
          memory_dims[3] = file_dims[3];
          
          file_counts[0] = file_dims[0]/p;
          file_counts[1] = file_dims[1];
          file_counts[2] = file_dims[2];
          file_counts[3] = file_dims[3];
          slice_npix = ptr_itn_array_struct->npix/p;
#else /* no USE_MPI */
          memory_dims[0] = 1;
          memory_dims[1] = file_dims[1];
          memory_dims[2] = file_dims[2];
          memory_dims[3] = file_dims[3];
          
          file_counts[0] = 1;
          file_counts[1] = memory_dims[1];
          file_counts[2] = memory_dims[2];
          file_counts[3] = memory_dims[3];
          slice_npix = (dm_array_index_t)(ptr_itn_array_struct->nx) *
              (dm_array_index_t)(ptr_itn_array_struct->ny);
#endif /* USE_MPI */
      }
      free(file_dims);
      
      if ((memory_dataspace = H5Screate_simple(file_n_dims, memory_dims, 
                                               NULL)) < 0) {
          strcpy(error_string,"H5Screate_simple(itn_array,memory) error");
          H5Sclose(file_dataspace);
          H5Sclose(memory_dataspace);
          H5Tclose(read_datatype);
          H5Gclose(itn_group);
          return(DM_FILEIO_FAILURE);
      }
  } /* endif(my_rank == 0) */
  
  /* Now here is where we have to be careful for split versus
   * interleaved complex arrays.  We take care of this by
   * always making an interleaved array.  Remember that
   * we should not use sizeof(dm_array_complex) because it
   * might be an array of pointers.
   */
#if USE_MPI
  /* Get the data from the nodes. npix will tell us the total number of
   * elements so we need to divide by the number of processes..
   */
      for (i = 0; i < p; i++) {
      if (my_rank == 0) {
              
          slice_array =
              (dm_array_real *)malloc(2*sizeof(dm_array_real)*
                                      ptr_itn_array_struct->npix/p);

          /* Determine the file_offsets here. Note that we have to divide by
           * number of processes. Note that we always have at least 2
           * dimensions because it is complex.
           */
          if (file_n_dims == 2) {
              file_offsets[0] = i*ptr_itn_array_struct->nx/p;
          } else if (file_n_dims == 3) {
              file_offsets[0] = i*ptr_itn_array_struct->ny/p;
          } else if (file_n_dims == 4) {
              file_offsets[0] = i*ptr_itn_array_struct->nz/p;
          }
              
          if ((status = H5Sselect_hyperslab(file_dataspace, H5S_SELECT_SET,
                                            file_offsets, NULL,
                                            file_counts, NULL)) < 0) {
              strcpy(error_string,
                     "Error in H5Sselect_hyperslab(itn_array)");
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              H5Tclose(read_datatype);
              H5Gclose(itn_group);
              free(slice_array);
              return(DM_FILEIO_FAILURE);
          }

          /* Always read into slice array */
          if ((status = H5Dread(dataset, read_datatype, memory_dataspace,
                                file_dataspace,H5P_DEFAULT,
                                slice_array)) < 0) {
              strcpy(error_string, "Error in H5Dwrite (itn_array)!");
              H5Sclose(file_dataspace);
              H5Sclose(memory_dataspace);
              H5Tclose(datatype);
              H5Tclose(read_datatype);
              H5Gclose(itn_group);
              free(slice_array);
              return(DM_FILEIO_FAILURE);
          }

          if (i == 0) {
              /* Map directly into itn_array_struct */
              for (ipix=0; ipix<ptr_itn_array_struct->npix/p; ipix++) {
                  c_re(ptr_itn_array_struct->complex_array,ipix) =
                      (*(slice_array+2*ipix));
                  c_im(ptr_itn_array_struct->complex_array,ipix) =
                      (*(slice_array+2*ipix+1));
              }
          } else {
              /* Send */
              MPI_Send(slice_array,2*ptr_itn_array_struct->npix/p,MPI_ARRAY_REAL,
                       i,99,MPI_COMM_WORLD);
          } /* endif(i == 0) */
          free(slice_array);
      }  /* endif(my_rank == 0) */ 

      if (i > 0) {
          if (my_rank == i) {
              /* the slice array will always be interleaved */
              slice_array =
                  (dm_array_real *)malloc(2*sizeof(dm_array_real)*
                                          ptr_itn_array_struct->npix/p);
              MPI_Recv(slice_array,2*ptr_itn_array_struct->npix/p,MPI_ARRAY_REAL,
                       0,99,MPI_COMM_WORLD,&mpi_status);
              
              /* Now map using the predefined macros */
              for (ipix=0; ipix<ptr_itn_array_struct->npix/p; ipix++) {
                  c_re(ptr_itn_array_struct->complex_array,ipix) =
                      (*(slice_array+2*ipix));
                  c_im(ptr_itn_array_struct->complex_array,ipix) =
                      (*(slice_array+2*ipix+1));
              }
              free(slice_array);
          } /* endif(my_rank == i) */
      } /* endif(i > 0) */
  } /* endfor */

  
#else /* no USE_MPI */
  slice_array = (dm_array_real *)malloc(2*sizeof(dm_array_real)*slice_npix);
  if (slice_array == NULL) {
    strcpy(error_string,"slice malloc() error");
    H5Dclose(dataset);
    H5Sclose(file_dataspace);
    H5Sclose(memory_dataspace);
    H5Tclose(read_datatype);
    H5Gclose(itn_group);
    return(DM_FILEIO_FAILURE);
  }
  for (iz=0; iz<(ptr_itn_array_struct->nz); iz++) {
    file_offsets[0] = iz;
    if ((status = H5Sselect_hyperslab(file_dataspace,H5S_SELECT_SET,
				      file_offsets,NULL,
				      file_counts,NULL)) < 0) {
      strcpy(error_string,"Error in H5Sselect_hyperslab(itn_array)");
      H5Dclose(dataset);
      H5Sclose(file_dataspace);
      H5Sclose(memory_dataspace);
      H5Tclose(read_datatype);
      H5Gclose(itn_group);
      return(DM_FILEIO_FAILURE);
    }
    if ((status = H5Dread(dataset,read_datatype,memory_dataspace,
			  file_dataspace,H5P_DEFAULT,slice_array)) < 0) {
      strcpy(error_string,"Error in H5Dwrite(itn_array)");
      H5Dclose(dataset);
      H5Sclose(file_dataspace);
      H5Sclose(memory_dataspace);
      H5Tclose(read_datatype);
      H5Gclose(itn_group);
      return(DM_FILEIO_FAILURE);
    }

    zoffset = iz*slice_npix;
    for (ipix=0; ipix<slice_npix; ipix++) {
      c_re(ptr_itn_array_struct->complex_array,(ipix+zoffset)) =
	(*(slice_array+2*ipix));
      c_im(ptr_itn_array_struct->complex_array,(ipix+zoffset)) =
	(*(slice_array+2*ipix+1));
    }
  }

  free(slice_array);
#endif /* USE_MPI */

  if (my_rank == 0) {
      H5Dclose(dataset);
      H5Sclose(file_dataspace);
      H5Sclose(memory_dataspace);
      H5Tclose(read_datatype);
      H5Gclose(itn_group);
  }
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
void dm_h5_insert_adi_struct_members(hid_t datatype)
{
  H5Tinsert(datatype,"photon_scaling", 
	    HOFFSET(dm_adi_struct,photon_scaling),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"error_scaling", 
	    HOFFSET(dm_adi_struct,error_scaling),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"lambda_meters", 
	    HOFFSET(dm_adi_struct,lambda_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"camera_z_meters", 
	    HOFFSET(dm_adi_struct,camera_z_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"camera_x_pixelsize_meters", 
	    HOFFSET(dm_adi_struct,camera_x_pixelsize_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"camera_y_pixelsize_meters", 
	    HOFFSET(dm_adi_struct,camera_y_pixelsize_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"camera_alpha_radians", 
	    HOFFSET(dm_adi_struct,camera_alpha_radians),
	    H5T_NATIVE_DOUBLE); 
  H5Tinsert(datatype,"camera_beta_radians", 
	    HOFFSET(dm_adi_struct,camera_beta_radians),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"camera_gamma_radians", 
	    HOFFSET(dm_adi_struct,camera_gamma_radians),
	    H5T_NATIVE_DOUBLE); 
  H5Tinsert(datatype,"median_filter_width", 
	    HOFFSET(dm_adi_struct,median_filter_width),
	    H5T_NATIVE_DOUBLE); 
  H5Tinsert(datatype,"median_filter_threshold", 
	    HOFFSET(dm_adi_struct,median_filter_threshold),
	    H5T_NATIVE_DOUBLE); 
  H5Tinsert(datatype,"saturation_min", 
	    HOFFSET(dm_adi_struct,saturation_min),
	    H5T_NATIVE_DOUBLE); 
  H5Tinsert(datatype,"saturation_max", 
	    HOFFSET(dm_adi_struct,saturation_max),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"theta_x_radians", 
	    HOFFSET(dm_adi_struct,theta_x_radians),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"theta_y_radians", 
	    HOFFSET(dm_adi_struct,theta_y_radians),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"theta_z_radians", 
	    HOFFSET(dm_adi_struct,theta_z_radians),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"xcenter_offset_pixels", 
	    HOFFSET(dm_adi_struct,xcenter_offset_pixels),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"ycenter_offset_pixels", 
	    HOFFSET(dm_adi_struct,ycenter_offset_pixels),
	    H5T_NATIVE_DOUBLE);
}

/*-------------------------------------------------------------------------*/
void dm_h5_insert_spt_struct_members(hid_t datatype)
{
  H5Tinsert(datatype,"support_scaling", 
	    HOFFSET(dm_spt_struct,support_scaling),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"pix_x_meters", 
	    HOFFSET(dm_spt_struct,pix_x_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"pix_y_meters", 
	    HOFFSET(dm_spt_struct,pix_y_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"pix_z_meters", 
	    HOFFSET(dm_spt_struct,pix_z_meters),
	    H5T_NATIVE_DOUBLE);
}

/*-------------------------------------------------------------------------*/
void dm_h5_insert_itn_struct_members(hid_t datatype)
				     
{

  H5Tinsert(datatype,"photon_scaling", 
	    HOFFSET(dm_itn_struct,photon_scaling),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"pix_x_meters", 
	    HOFFSET(dm_itn_struct,pix_x_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"pix_y_meters", 
	    HOFFSET(dm_itn_struct,pix_y_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"pix_z_meters", 
	    HOFFSET(dm_itn_struct,pix_z_meters),
	    H5T_NATIVE_DOUBLE);
  H5Tinsert(datatype,"iterate_count", 
	    HOFFSET(dm_itn_struct,iterate_count),
	    H5T_NATIVE_UINT32);
 
}

/*-------------------------------------------------------------------------*/
void dm_clear_comments(dm_comment_struct *ptr_comment_struct)
{
  size_t i_char, total_bytes;

  total_bytes = ptr_comment_struct->n_strings_max *
      ptr_comment_struct->string_length;
  for (i_char=0; i_char<total_bytes; i_char++) {
      *(ptr_comment_struct->string_array+i_char) = '\000';
  }
  for (i_char=0; i_char<(ptr_comment_struct->string_length); i_char++) {
      *(ptr_comment_struct->specimen_name+i_char) = '\000';
      *(ptr_comment_struct->collection_date+i_char) = '\000';
  }
  ptr_comment_struct->n_strings = 0;
}

/*-------------------------------------------------------------------------*/
void dm_add_string_to_comments(char *string_to_add,
			       dm_comment_struct *ptr_comment_struct)
{
  int i_char, i_offset, this_strlen, last_index;
  char this_char;

  /* Do nothing if we have no room to add the string */
  if (((ptr_comment_struct->n_strings)+1) > 
      (ptr_comment_struct->n_strings_max)) return;

  /* Only copy characters up to the end of each string length, no matter
   * how long string_to_add is.
   */
  this_strlen = strlen(string_to_add);
  i_offset = (ptr_comment_struct->n_strings)*
    (ptr_comment_struct->string_length);
  i_char = 0;
  last_index = ptr_comment_struct->string_length-1;
  while (i_char < last_index) {
    this_char = *(string_to_add+i_char);
    if ((i_char < this_strlen) && (this_char != '\n') &&
	(this_char != '\r')) {
      *(ptr_comment_struct->string_array+i_char+i_offset) = this_char;
    } else {
      *(ptr_comment_struct->string_array+i_char+i_offset) = '\000';
    }
    i_char++;
  }
  /* Make sure the string is null-terminated */
  *(ptr_comment_struct->string_array+last_index+i_offset) = '\000';
  ptr_comment_struct->n_strings += 1;
}

/*-------------------------------------------------------------------------*/
void dm_add_specimen_name_to_comments(char *string_to_add,
				      dm_comment_struct *ptr_comment_struct)
{
  int i_char, this_strlen, last_index;
  char this_char;

  /* Only copy characters up to the end of each string length, no matter
   * how long string_to_add is.
   */
  this_strlen = strlen(string_to_add);
  last_index = (ptr_comment_struct->string_length)-1;
  i_char = 0;
  while (i_char < last_index) {
    this_char = *(string_to_add+i_char);
    if ((i_char < this_strlen) && (this_char != '\n') &&
	(this_char != '\r')) {
      *(ptr_comment_struct->specimen_name+i_char) = this_char;
    } else {
      *(ptr_comment_struct->specimen_name+i_char) = '\000';
    }
    i_char++;
  }
  /* Make sure the string is null-terminated */
  *(ptr_comment_struct->specimen_name+last_index) = '\000';
}

/*-------------------------------------------------------------------------*/
void dm_add_collection_date_to_comments(char *string_to_add,
					dm_comment_struct *ptr_comment_struct)
{
  int i_char, this_strlen, last_index;
  char this_char;

  /* Only copy characters up to the end of each string length, no matter
   * how long string_to_add is.
   */
  this_strlen = strlen(string_to_add);
  last_index = (ptr_comment_struct->string_length)-1;
  i_char = 0;
  while (i_char < last_index) {
    this_char = *(string_to_add+i_char);
    if ((i_char < this_strlen) && (this_char != '\n') &&
	(this_char != '\r')) {
      *(ptr_comment_struct->collection_date+i_char) = this_char;
    } else {
      *(ptr_comment_struct->collection_date+i_char) = '\000';
    }
    i_char++;
  }
  /* Make sure the string is null-terminated */
  *(ptr_comment_struct->collection_date+last_index) = '\000';
}

/*-------------------------------------------------------------------------*/
void dm_clear_local_string_array(char *local_string_array,
				 int string_length, 
				 int n_frames)
{
  size_t i_char, total_bytes;
  total_bytes = string_length * n_frames;
  for (i_char=0; i_char<total_bytes; i_char++) {
    *local_string_array = '\000';
  }
}

/*-------------------------------------------------------------------------*/
void dm_clear_ainfo(dm_ainfo_struct *ptr_ainfo_struct)
{
   size_t i_char, total_bytes;

   total_bytes = ptr_ainfo_struct->n_frames_max *
       ptr_ainfo_struct->string_length;
   for (i_char=0; i_char<total_bytes; i_char++) {
       *(ptr_ainfo_struct->filename_array+i_char) = '\000';
       *(ptr_ainfo_struct->systime_array+i_char) = '\000';
   }
   for (i_char=0; i_char<(ptr_ainfo_struct->string_length); i_char++) {
       *(ptr_ainfo_struct->file_directory+i_char) = '\000';
   }
   for (i_char=0; i_char<(ptr_ainfo_struct->n_frames_max); i_char++) {
       *(ptr_ainfo_struct->theta_x_radians_array) = (double) 0;
       *(ptr_ainfo_struct->xcenter_offset_pixels_array) = (double) 0;
       *(ptr_ainfo_struct->ycenter_offset_pixels_array) = (double) 0;  
   }
   ptr_ainfo_struct->n_frames = 0;
   ptr_ainfo_struct->file_directory_flag = 0;
   ptr_ainfo_struct->filenames_offset = 0;
   ptr_ainfo_struct->systimes_offset = 0;
   ptr_ainfo_struct->theta_x_offset = 0;
   ptr_ainfo_struct->xcenter_offset = 0;
   ptr_ainfo_struct->ycenter_offset = 0;
}

/*-------------------------------------------------------------------------*/
void dm_add_file_directory_to_ainfo(char *directory_to_add,
				    dm_ainfo_struct *ptr_ainfo_struct)
{
  int this_strlen, last_index, i_char;
  char this_char;

  /* Only copies characters until maximum string length is reached.
   */
  this_strlen = strlen(directory_to_add);
  last_index = (ptr_ainfo_struct->string_length)-1;
  i_char = 0;
  while (i_char < last_index) {
    this_char = *(directory_to_add+i_char);
    if ((i_char < this_strlen) && (this_char != '\n') &&
	(this_char != '\r')) {
      *(ptr_ainfo_struct->file_directory+i_char) = this_char;
    } else {
      *(ptr_ainfo_struct->file_directory+i_char) = '\000';
    }
    i_char++;
  }
  /* Make sure the string is null-terminated */
  *(ptr_ainfo_struct->file_directory+last_index) = '\000';
  ptr_ainfo_struct->file_directory_flag++;
}
/*-------------------------------------------------------------------------*/
void dm_add_filename_to_ainfo(char *filename_to_add,
			      dm_ainfo_struct *ptr_ainfo_struct)
{
  int i_char, i_offset, this_strlen, last_index;
  char this_char;

  /* Do nothing if we have no room to add the string */
  if (((ptr_ainfo_struct->filenames_offset)+1) > 
      (ptr_ainfo_struct->n_frames_max)) return;

  /* Only copy characters up to the end of each string length, no matter
   * how long filename_to_add is.
   */
  this_strlen = strlen(filename_to_add);
  i_offset = (ptr_ainfo_struct->filenames_offset)*
    (ptr_ainfo_struct->string_length);
  i_char = 0;
  last_index = ptr_ainfo_struct->string_length-1;
  while (i_char < last_index) {
    this_char = *(filename_to_add+i_char);
    if ((i_char < this_strlen) && (this_char != '\n') &&
	(this_char != '\r')) {
      *(ptr_ainfo_struct->filename_array+i_char+i_offset) = this_char;
    } else {
      *(ptr_ainfo_struct->filename_array+i_char+i_offset) = '\000';
    }
    i_char++;
  }
  /* Make sure the string is null-terminated */
  *(ptr_ainfo_struct->filename_array+last_index+i_offset) = '\000';
  ptr_ainfo_struct->filenames_offset += 1;

}

/*-------------------------------------------------------------------------*/
int dm_add_double_to_ainfo(char *tagname,
			   double double_to_add,
			   dm_ainfo_struct *ptr_ainfo_struct,
			   char *error_string)
{
  /* Check tagname first to determine which double array is to be edited */
  if (strcmp(tagname,"theta_x") == 0) {
    *(ptr_ainfo_struct->theta_x_radians_array 
      + ptr_ainfo_struct->theta_x_offset) = double_to_add;
    ptr_ainfo_struct->theta_x_offset++;
  } else if (strcmp(tagname,"xcenter") == 0) {
    *(ptr_ainfo_struct->xcenter_offset_pixels_array 
      + ptr_ainfo_struct->xcenter_offset) = double_to_add;
    ptr_ainfo_struct->xcenter_offset++;
  } else if (strcmp(tagname,"ycenter") == 0) {
    *(ptr_ainfo_struct->ycenter_offset_pixels_array 
       + ptr_ainfo_struct->ycenter_offset) = double_to_add;
     ptr_ainfo_struct->ycenter_offset++;
  } else {
    strcpy(error_string,
	   "dm_add_double_to_ainfo: No matching array for given tagname.");
    return(DM_FILEIO_FAILURE);
  }

  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
int dm_read_ainfo_from_csv(char *csv_filename,
			   dm_ainfo_struct *ptr_ainfo_struct,
			   char *error_string)
{
  int i_char, i_array, offset, i, count, i_split, ainfo_tags, tag_count;
  FILE *fp;
  char *local_filename_array, this_char;
  char *temp_string, *split_string[AINFO_TAGS];
  size_t n_chars;
  double *temp_double;
  
  n_chars = 
    (int)ptr_ainfo_struct->n_frames_max*ptr_ainfo_struct->string_length;
  if ((fp = fopen(csv_filename,"r")) == NULL) {
    strcpy(error_string,"Error opening csv_filename.\n");
    return(DM_FILEIO_FAILURE);
  }

  local_filename_array = (char *)malloc(n_chars);

  /* Read file character by character into local_filename_array */
  count = 0;
  while (fscanf(fp,"%c", (local_filename_array +count)) == 1) {
    count++;
    if (count == n_chars -1) {
      strcpy(error_string, 
	     "dm_read_ainfo_from_csv: Size of list of filenames exceeds allocated memory.");
      free(local_filename_array);
      fclose(fp);
      return(DM_FILEIO_FAILURE);
    }
  }
  fclose(fp);
  
  tag_count = 0;
  i_split = 0;
  split_string[0] = strtok(local_filename_array,"\n");
  while (split_string[i_split] != NULL) {
    split_string[i_split+1] = strtok(NULL,"\n");
    i_split++;
    /* Make sure we don't try to use more pointer than we allocated */
    if (i_split == ptr_ainfo_struct->ainfo_tags) {
      break;
    }
  }
  
  for (i=0;i<i_split;i++) {
    if (strncmp(split_string[i], "directory", 9) == 0) {
      /* Split at '=' sign, then move pointer to next character */
      temp_string = strchr(split_string[i],'=');
      temp_string++;
      dm_add_file_directory_to_ainfo(temp_string,ptr_ainfo_struct);
    } else if (strncmp(split_string[i], "filenames", 9) == 0) {
      temp_string = strtok(split_string[i],",");
      temp_string = strchr(temp_string,'=');
      temp_string++;
      while (temp_string != NULL) {
	dm_add_filename_to_ainfo(temp_string, ptr_ainfo_struct);
	temp_string = strtok(NULL,",");
      }
    } else if (strncmp(split_string[i], "systimes", 8) == 0) {
      temp_string = strtok(split_string[i],",");
      temp_string = strchr(temp_string,'=');
      temp_string++;
      while (temp_string != NULL) {
	dm_add_systime_to_ainfo(temp_string, ptr_ainfo_struct);
	temp_string = strtok(NULL,",");
      }
    } else if (strncmp(split_string[i], "gmr_x", 5) == 0) {
      temp_string = strtok(split_string[i],",");
      temp_string = strchr(temp_string,'=');
      temp_string++;
      while (temp_string != NULL) {
	*(ptr_ainfo_struct->theta_x_radians_array + 
	  ptr_ainfo_struct->theta_x_offset) = 
	  PI*strtod(temp_string,NULL)/(180.0);
	ptr_ainfo_struct->theta_x_offset++;
	temp_string = strtok(NULL,",");
      }
    } else if (strncmp(split_string[i], "xcenter", 7) == 0) {
      temp_string = strtok(split_string[i],",");
      temp_string = strchr(temp_string,'=');
      temp_string++;
      while (temp_string != NULL) {
	*(ptr_ainfo_struct->xcenter_offset_pixels_array + 
	  ptr_ainfo_struct->xcenter_offset) = strtod(temp_string,NULL);
	ptr_ainfo_struct->xcenter_offset++;
	temp_string = strtok(NULL,",");
      }
    } else if (strncmp(split_string[i], "ycenter", 7) == 0) {
      temp_string = strtok(split_string[i],",");
      temp_string = strchr(temp_string,'=');
      temp_string++;
      while (temp_string != NULL) {
	*(ptr_ainfo_struct->ycenter_offset_pixels_array + 
	  ptr_ainfo_struct->ycenter_offset) = strtod(temp_string,NULL);
	ptr_ainfo_struct->ycenter_offset++;
	temp_string = strtok(NULL,",");
      }
    } else {
      /* Count the tags that did not match any of the above statements*/
      tag_count++;
    }
  }
  
  /* If the number of not matching tags equals the total number of tags,
   * then the csv file is not good.
   */
  if (tag_count == i_split) {
    strcpy(error_string,
	   "dm_read_ainfo_from_csv: File does not contain any matching arguments.\n");
    return(DM_FILEIO_FAILURE);
  }
  
  free(local_filename_array);
  return(DM_FILEIO_SUCCESS);
}

/*-------------------------------------------------------------------------*/
void dm_add_systime_to_ainfo(char *systime_to_add,
			     dm_ainfo_struct *ptr_ainfo_struct)
{
  int i_char, i_offset, this_strlen, last_index;
  char this_char;

  /* Do nothing if we have no room to add the string */
  if (((ptr_ainfo_struct->systimes_offset)+1) > 
      (ptr_ainfo_struct->n_frames_max)) return;
 

  /* Only copy characters up to the end of each string length, no matter
   * how long filename_to_add is.
   */
  this_strlen = strlen(systime_to_add);
  i_offset = (ptr_ainfo_struct->systimes_offset)*
    (ptr_ainfo_struct->string_length);
  i_char = 0;
  last_index = ptr_ainfo_struct->string_length-1;
  while (i_char < last_index) {
    this_char = *(systime_to_add+i_char);
    if ((i_char < this_strlen) && (this_char != '\n') &&
	(this_char != '\r')) {
      *(ptr_ainfo_struct->systime_array+i_char+i_offset) = this_char;
    } else {
      *(ptr_ainfo_struct->systime_array+i_char+i_offset) = '\000';
    }
    i_char++;
  }
  /* Make sure the string is null-terminated */
  *(ptr_ainfo_struct->systime_array+last_index+i_offset) = '\000';
  ptr_ainfo_struct->systimes_offset += 1;

}

/*-------------------------------------------------------------------------*/
void dm_print_comments(FILE *fp_out, 
		       char *preceding_string,
		       dm_comment_struct *ptr_comment_struct,
		       char *trailing_string,
                       int my_rank)
{
  int i_string, i_offset;

  if (my_rank == 0) {
      fprintf(fp_out,"%s%s%s",preceding_string,
              ptr_comment_struct->specimen_name,trailing_string);
      fprintf(fp_out,"%s%s%s",preceding_string,
	  ptr_comment_struct->collection_date,trailing_string);
      for (i_string=0; i_string<(ptr_comment_struct->n_strings); i_string++) {
          i_offset = i_string*(ptr_comment_struct->string_length);
          fprintf(fp_out,"%s%s%s",preceding_string,
                  ((ptr_comment_struct->string_array)+i_offset),trailing_string);
      }
  } /* endif(my_rank == 0) */
}

/*-------------------------------------------------------------------------*/
void dm_print_ainfo(FILE *fp_out, 
		    char *preceding_string,
		    dm_ainfo_struct *ptr_ainfo_struct,
		    char *trailing_string,
                    int my_rank)
{
  int i_string, i_offset;

  if (my_rank == 0) {

      fprintf(fp_out,"%s%s%s\n",preceding_string,
              ptr_ainfo_struct->file_directory,trailing_string);
      for (i_string=0; i_string<(ptr_ainfo_struct->n_frames); i_string++) {
          i_offset = i_string*(ptr_ainfo_struct->string_length);
          fprintf(fp_out,"%s%s%s",preceding_string,
                  ((ptr_ainfo_struct->filename_array)+i_offset),trailing_string);
      }
      fprintf(fp_out,"\n");

      for (i_string=0; i_string<(ptr_ainfo_struct->n_frames); i_string++) {
          i_offset = i_string*(ptr_ainfo_struct->string_length);
          fprintf(fp_out,"%s%s%s",preceding_string,
                  ((ptr_ainfo_struct->systime_array)+i_offset),trailing_string);
      }
      fprintf(fp_out,"\n");


      for (i_string=0; i_string<(ptr_ainfo_struct->n_frames); i_string++) {
          fprintf(fp_out,"%s%f%s",preceding_string,
                  *((ptr_ainfo_struct->theta_x_radians_array)+i_string),
                  trailing_string);
      }
      fprintf(fp_out,"\n");

      for (i_string=0; i_string<(ptr_ainfo_struct->n_frames); i_string++) {
          fprintf(fp_out,"%s%f%s",preceding_string,
                  *((ptr_ainfo_struct->xcenter_offset_pixels_array)+i_string),
		  trailing_string);
      }
      fprintf(fp_out,"\n");
  
      for (i_string=0; i_string<(ptr_ainfo_struct->n_frames); i_string++) {
          fprintf(fp_out,"%s%f%s",preceding_string,
                  *((ptr_ainfo_struct->ycenter_offset_pixels_array)+i_string),
		  trailing_string);
      }
      fprintf(fp_out,"\n");

      fprintf(fp_out,"%s%d%s",preceding_string,
              ptr_ainfo_struct->no_background,trailing_string);
      fprintf(fp_out,"\n");
  
      fprintf(fp_out,"%s%d%s",preceding_string,
              ptr_ainfo_struct->dk_by_pix,trailing_string);
      fprintf(fp_out,"\n");
  
      fprintf(fp_out,"%s%d%s",preceding_string,
              ptr_ainfo_struct->merge_first,trailing_string);
      fprintf(fp_out,"\n");
  } /* endif(my_rank == 0) */
}     

/*-------------------------------------------------------------------------*/
int dm_check_ainfo(dm_ainfo_struct *ptr_ainfo_struct,
		   char *error_string,
                   int my_rank)
{
    int i_array;

    if (my_rank == 0) {
        /* First check that no offset is greater than n_frames */
        if (ptr_ainfo_struct->filenames_offset > ptr_ainfo_struct->n_frames) {
            strcpy(error_string,"dm_check_ainfo: filename_array too long");
            return(DM_FILEIO_FAILURE);
        }
  
        if (ptr_ainfo_struct->systimes_offset > ptr_ainfo_struct->n_frames) {
            strcpy(error_string,"dm_check_ainfo: systime_array too long");
            return(DM_FILEIO_FAILURE);
        }

        if (ptr_ainfo_struct->theta_x_offset > ptr_ainfo_struct->n_frames) {
            strcpy(error_string,
		   "dm_check_ainfo: theta_x_radians_array too long");
            return(DM_FILEIO_FAILURE);
        }

        if (ptr_ainfo_struct->xcenter_offset > ptr_ainfo_struct->n_frames) {
            strcpy(error_string,"dm_check_ainfo: xcenter_array too long");
            return(DM_FILEIO_FAILURE);
        }

        if (ptr_ainfo_struct->ycenter_offset > ptr_ainfo_struct->n_frames) {
            strcpy(error_string,"dm_check_ainfo: ycenter_array too long");
            return(DM_FILEIO_FAILURE);
        }


        /* Now check length of arrays and add default values if necessary */

        if (!ptr_ainfo_struct->file_directory_flag) {
            dm_add_file_directory_to_ainfo(" ", ptr_ainfo_struct);
        }
  
        if (ptr_ainfo_struct->filenames_offset < ptr_ainfo_struct->n_frames) {
            for (i_array=ptr_ainfo_struct->filenames_offset; 
                 i_array < ptr_ainfo_struct->n_frames; i_array++ ) {
                dm_add_filename_to_ainfo(" ", ptr_ainfo_struct);
            }
        }

        if (ptr_ainfo_struct->systimes_offset < ptr_ainfo_struct->n_frames) {
            for (i_array=ptr_ainfo_struct->systimes_offset; 
                 i_array < ptr_ainfo_struct->n_frames; i_array++ ) {
                dm_add_systime_to_ainfo(" ", ptr_ainfo_struct);
            }
        }

        if (ptr_ainfo_struct->theta_x_offset < ptr_ainfo_struct->n_frames) {
            for (i_array=ptr_ainfo_struct->theta_x_offset; 
                 i_array < ptr_ainfo_struct->n_frames; i_array++ ) {
                *(ptr_ainfo_struct->theta_x_radians_array +i_array) = -9999.99;
                ptr_ainfo_struct->theta_x_offset++;
            }
        }

        if (ptr_ainfo_struct->xcenter_offset < ptr_ainfo_struct->n_frames) {
            for (i_array=ptr_ainfo_struct->xcenter_offset; 
                 i_array < ptr_ainfo_struct->n_frames; i_array++ ) {
                *(ptr_ainfo_struct->xcenter_offset_pixels_array +i_array) = 
		  -9999.99;
                ptr_ainfo_struct->xcenter_offset++;
            }
        }

        if (ptr_ainfo_struct->ycenter_offset < ptr_ainfo_struct->n_frames) {
            for (i_array=ptr_ainfo_struct->ycenter_offset; 
                 i_array < ptr_ainfo_struct->n_frames; i_array++ ) {
                *(ptr_ainfo_struct->ycenter_offset_pixels_array +i_array) = 
		  -9999.99;
                ptr_ainfo_struct->ycenter_offset++;
            }
        }
    } /* endif(my_rank == 0) */
  
  return(DM_FILEIO_SUCCESS);
}


