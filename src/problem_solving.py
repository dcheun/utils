#!/usr/bin/env python

"""Problem Solving.

Miscellaneous solutions for solving various problems requiring critical thinking.

"""

__author__ = "Danny Cheun"
__credits__ = ["Danny Cheun"]
__version__ = "1.0.0"
__maintainer__ = "Danny Cheun"
__email__ = "dcheun@gmail.com"


def find_dice_probability(roll_num, num_dice, num_sides):
    """Find probability of rolling the number "roll_num" given the number
    of dice and sides of each die.
    
    A die can have more than 6 sides, and you can be given more than 2 dice.
    
    Eg: Given 2 6-sided dice, what is the probability of rolling a 7? Ans: 0.16667
    Eg: Given 3 8-sided dice, what is the probability of rolling a 13? Ans: 0.09375
    
    @return: The probability as a float.
    
    """
    total_possible = num_sides ** num_dice
    num_occurrences = 0
    # Create initial matrix of dice
    dice_pool = [range(1, num_sides+1)] * num_dice
    # All combinations matrix - The cartesian set.
    combinations = [[]]
    for dice in dice_pool:
        combinations = [x+[y] for x in combinations for y in dice]
    # Loop through all combinations, add them up, count occurrences of roll_num.
    for combo in combinations:
        if sum(combo) == roll_num:
            num_occurrences += 1
    return '%.5f' % (float(num_occurrences) / total_possible)
