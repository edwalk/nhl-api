# Code below is useful to iterate through all available seasonIds for API calls.

firstYear = 1917
secondYear = 1918

while firstYear <= 2023 and secondYear <= 2024:

    id = str(firstYear) + str(secondYear)
    print(id)
    firstYear += 1
    secondYear += 1

    