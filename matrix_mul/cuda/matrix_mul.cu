/*
    Copyright (C) 2011  Abhinav Jauhri (abhinav.jauhri@gmail.com), Carnegie Mellon University - Silicon Valley 

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/


#include <cuda.h>
#include <cuda_runtime.h>
#include "matrix_mul.h"
#define TILE_SZ 4 
#define BLK_SZ 16

namespace cuda
{
  __global__ 
  void 
  matrix_mul_kernel(float *sq_matrix_1, float *sq_matrix_2, float *sq_matrix_result, int sq_dimension)
  {
    int i,j,k;
    int tx = threadIdx.x;
    int ty = threadIdx.y;
    int col = blockIdx.x * TILE_SZ * BLK_SZ + threadIdx.x;
    int row = blockIdx.y * BLK_SZ + threadIdx.y;

    __shared__ float mat1[BLK_SZ][BLK_SZ];
    __shared__ float mat2[BLK_SZ][BLK_SZ*TILE_SZ]; 

    float sum[TILE_SZ];

    for (j = 0; j < TILE_SZ; j ++)
	sum[j] = 0.0;
   
    for (k = 0; k < (sq_dimension + BLK_SZ - 1)/BLK_SZ; k ++)
    {
    	if (k*BLK_SZ + tx < sq_dimension && row < sq_dimension)
    	{
    		mat1[ty][tx] = sq_matrix_1[row*sq_dimension + k * BLK_SZ + tx];
    	}
    	else
    	{
    		mat1[ty][tx] = 0.0;
    	}

	for (j = 0; j < TILE_SZ; j ++)
	{
		if (k*BLK_SZ + ty < sq_dimension && col + j*BLK_SZ < sq_dimension)
		{
    			mat2[ty][tx + j*BLK_SZ] = sq_matrix_2[(ty + k*BLK_SZ)*sq_dimension + col + j*BLK_SZ];
		}
		else
		{
    			mat2[ty][tx + j*BLK_SZ] = 0.0;
		}
	}

		
	__syncthreads();
	for (j = 0; j < TILE_SZ; j ++)
	{
    		for(i = 0; i < BLK_SZ; i++)
    		{
			sum[j] += mat1[ty][i]*mat2[i][tx + j*BLK_SZ];
    		}
	}
	__syncthreads();
    }

    for (j = 0; j < TILE_SZ; j ++)
    {
    	if (row < sq_dimension && col + j*BLK_SZ <  sq_dimension)	
    		sq_matrix_result[row*sq_dimension + col + j*BLK_SZ] = sum[j];
    }
   
  }
  
  void 
  matrix_multiplication(float *sq_matrix_1, float *sq_matrix_2, float *sq_matrix_result, unsigned int sq_dimension)
  {
    int size = sq_dimension * sq_dimension * sizeof(float);
    float *sq_matrix_1_d, *sq_matrix_2_d, *sq_matrix_result_d;
    
    /***************************************************
  1st Part: Allocation of memory on device memory  
    ****************************************************/
    
    /* copy sq_matrix_1 and sq_matrix_2 to device memory */
    cudaMalloc((void**) &sq_matrix_1_d, size);
    cudaMemcpy(sq_matrix_1_d, sq_matrix_1, size, cudaMemcpyHostToDevice);
    cudaMalloc((void**) &sq_matrix_2_d, size);
    cudaMemcpy(sq_matrix_2_d, sq_matrix_2, size, cudaMemcpyHostToDevice);
    
    /*allocate sq_matrix_result on host */
    cudaMalloc((void**) &sq_matrix_result_d, size);
    
    /***************************************************
   2nd Part: Inovke kernel 
    ****************************************************/
    dim3 dimBlock(BLK_SZ, BLK_SZ);
    const unsigned int dimY = (sq_dimension + BLK_SZ - 1)/BLK_SZ;
    const unsigned int dimX = (sq_dimension + TILE_SZ*BLK_SZ - 1)/(TILE_SZ*BLK_SZ);
    dim3 dimGrid(dimX, dimY);
    matrix_mul_kernel<<<dimGrid, dimBlock, BLK_SZ*BLK_SZ*sizeof(float) + TILE_SZ*BLK_SZ*BLK_SZ*sizeof(float)>>>(sq_matrix_1_d, sq_matrix_2_d, sq_matrix_result_d, sq_dimension);
   
    /***************************************************
   3rd Part: Transfer result from device to host 
    ****************************************************/
    cudaMemcpy(sq_matrix_result, sq_matrix_result_d, size, cudaMemcpyDeviceToHost);
    cudaFree(sq_matrix_1_d);
    cudaFree(sq_matrix_2_d);
    cudaFree(sq_matrix_result_d);
  }  
} // namespace cuda
