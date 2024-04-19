# Assignment 3
Implement distributed K-means clustering using Hadoop MapReduce.

## Implementation Details

### 1. Master:
``` bash
    python master.py <num_of_mappers> <num_of_reducers> <num_of_iterations> <num_of_clusters>
```

#### Functions present in the master:
1. compile_centroids
2. run_iterations
3. rpc

#### Steps:
1. Takes input from the user.
2. Creates the input chunks and distributes them to the mappers.
3. Collects the output from the reducers.
4. Updates the centroids and repeats the process for the specified number of iterations.
5. Outputs the final centroids.

<b>Note:</b>
1. Split is distributed using the number of mappers - equally.
2. We have performed scenario 1, as we give the indices to mappers.

### 2. Mapper:
``` bash
    python mapper.py <mapper_id> <port>
```
#### Functions present in the mapper:
1. calculate_distance
2. map_function
3. write_partitions_to_files

#### Steps:
1. Reads the input from the master.
2. Calculates the distance of each point from the centroids.
3. Assigns the point to the nearest centroid.
4. Writes the output to the intermediate files.
5. Sends the data assigned to the reducer when asked.
   
<b>Note:</b> 
1. Each mapper runs as a different process.
2. Mappers are running in parallel.
3. By the partition function, keys are distributed equally using (key % num_of_reducers). 

### 3. Reducer:
``` bash
    python reducer.py <reducer_id> <port>
```

#### Functions present in the reducer:
1. calculate_new_centroids
2. shuffle_and_sort
3. reduce_function

#### Steps:
1. Reads the input from the mapper.
2. Calculates the new centroids.
3. Writes the output to the intermediate files.
4. Sends the data to the master when done.

<b>Note:</b>
1. Each reducer runs as a different process.
2. Reducers are running in parallel.
3. Reducers obtain their information through gRPC calls to the mapper and don't have direct access to the intermediate files.
4. Master sent a reduce request to which the reducer replies with the data (the alternative scenario in the assignment).

## Testing: Configurations

    input:
    (# mappers, # reducers, # iterations, # clusters)
    
### Test 1:
    points = 

    0.4,7.2
    0.8,9.8
    -1.5,7.3
    8.1,3.4
    7.3,2.3
    9.1,3.1
    8.9,0.2
    11.5,-1.9
    10.2,0.5
    9.8,1.2
    8.5,2.7
    10.3,-0.3
    9.7,0.8
    8.3,2.9
    0.0,7.1
    0.9,9.6
    -1.6,7.4
    0.3,7.2
    0.7,9.9
    -1.7,7.5
    0.5,7.4
    0.9,9.7
    -1.8,7.6
    11.1,-1.5
    10.8,-0.6
    9.5,1.5
    8.7,2.4
    11.2,-1.2
    10.5,-0.1
    9.3,1.9
    8.6,2.6

    correct centroids = 

    -0.17500000000000002,8.141666666666667
    9.547368421052632,1.0473684210526315

Iteration 1:

    (3, 2, 50, 2)

    obtained output (correct): 
    [[-0.17500000000000002, 8.141666666666667], [9.54736842105263, 1.0473684210526315]]

Iteration 2:

    (2, 2, 50, 2)

    obtained output (correct): 
    [[-0.17500000000000002, 8.141666666666667], [9.54736842105263, 1.0473684210526315]]

Iteration 3:

    (2, 1, 50, 2)

    obtained output (correct): 
    [[-0.17500000000000002, 8.141666666666667], [9.54736842105263, 1.0473684210526315]]

<hr>

### Test 2:
    points = 

    2.4253966361924117, 14.200140998826638 
    81.430136979439, 50.33034624768575 
    12.630863179096908, 96.83125993903082 
    85.32433236307622, 38.85085293948647 
    10.948801251154682, 63.84346297199029 
    81.15927937347692, 57.750360036814705 
    31.557122052707076, 36.90041258086754 
    46.66353476332117, 42.536829675995506 
    30.2214411954406, 60.63826278594955 
    15.256211095947226, 62.08885022713485 
    65.92469227195072, 51.192695664924834 
    22.918984884832927, 86.29388886815464 
    37.59627649460666, 83.10255966867024 
    88.28421977908025, 12.067049557489574 
    41.94706427555632, 76.82403075144262 
    2.552573964893323, 17.536987084919097 
    24.281737705586114, 48.02641007037477 
    14.536067497498816, 51.3905894486027 
    12.289008223892928, 76.22457846480101 
    30.934672692775134, 62.34121834917595

    correct centroids = 

    23.86036925, 74.24312356
    80.42453215, 42.03826089
    20.3360721,  35.09856164

Iteration 1:

    (3, 3, 50, 3)

    obtained output (correct): 
    [[18.07933601024393, 46.32959272420461], [74.7976992550574, 42.12135568706614], [25.47643941159715, 83.85526353841986]]

Iteration 2:

    (3, 2, 50, 3)

    obtained output (nearly correct): 
    [[2.4889853005428675, 15.868564041872869], [25.521675793262812, 65.15710413863003], [80.42453215340463, 42.03826088928027]]

Iteration 3:

    (2, 3, 50, 3)

    obtained output (near correct): 
    [[80.42453215340463, 42.03826088928027], [2.4889853005428675, 15.868564041872869], [25.521675793262812, 65.15710413863003]]

<hr>

### Test 3:
    points = 

    1.4302438269507878, 2.4735967228568025 
    -6.051219043568199, 1.092345773013907 
    -2.078183604154898, -0.5406879561729525 
    3.577377459191938, 3.9250454691813186 
    -7.261989485326856, -9.656893255794362 
    2.795713182079199, -5.6866613630349825 
    4.448291226827738, -2.6351189124757095 
    1.376478335100881, -8.010722695141594 
    -9.367821869741311, -1.7839205011606953 
    -6.073698627717137, 0.4437876732667849 
    -2.1269269744731654, 3.7698885014347514 
    0.8106537569050687, -2.9607554246996735 
    -5.276144991837021, -7.03508666505196 
    -8.136641858654077, -4.46105486606649 
    1.9960903285915936, 4.574134964587465 
    -9.615722957464303, 1.8214400218876108 
    -9.409686759957934, -8.519244758242134 
    -4.55686835830387, 3.019857927300656 
    -1.2405680431342763, -6.331651819842987 
    4.564762036968727, -0.024148791855280294 
    -9.7411473823522, -7.86201261459685 
    -1.1387724592535342, -1.9731830417087366 
    -3.279264851148022, -6.531152677999264 
    -8.410254651701504, 0.7440001087711536 
    1.4971327987543468, -7.94646524255290

    correct centroids = 

    1.22402245, -1.64359458
    -7.2650384, -3.22732782

Iteration 1:

    (3, 3, 50, 2)

    obtained output (correct): 
    [[-7.265038403147703, -3.2273278195559705], [1.2240224515657234, -1.643594583801883]]

Iteration 2:

    (3, 2, 50, 2)

    obtained output (correct): 
    [[1.2240224515657234, -1.643594583801883], [-7.265038403147703, -3.2273278195559705]]

Iteration 3:

    (2, 3, 50, 2)

    obtained output (near correct): 
    [[-7.265038403147703, -3.2273278195559705], [1.2240224515657234, -1.643594583801883]]

## Team Members
1. Harshil (2021050) 
2. Abhay (2021508)
3. Sameer (2021093)

<!-- THE END -->