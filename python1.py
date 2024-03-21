
def multiply_list_elements(input_list):
    result = 1
    for num in input_list:
        result *= num
    return result

# Example list
my_list = [2, 3, 4, 5]
result = multiply_list_elements(my_list)
print(result)  # Output: 120