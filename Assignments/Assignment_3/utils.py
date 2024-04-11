def split_input_data(num_lines, num_mappers):
    '''
    Split input data into chunks for each mapper

    Input:
    - num_lines: Number of lines in the input data
    - num_mappers: Number of mappers

    Output:
    - List of tuples, where each tuple represents the start and end index of lines for a mapper
    example, [(0, 10), (10, 20), (20, 30)] for 30 lines and 3 mappers
    '''
    lines_per_mapper = num_lines // num_mappers
    input_splits = []
    start_line = 0
    for i in range(num_mappers):
        end_line = start_line + lines_per_mapper
        if i == num_mappers - 1:  # Last mapper gets remaining lines
            end_line = num_lines
        input_splits.append((start_line, end_line))
        start_line = end_line

    # testing
    # print("Input splits:", input_splits)
    return input_splits

