from indicators.fibonacci import Fibonacci


fib = Fibonacci()

levels = fib.calculate(

    high=65000,

    low=60000,

)

print()

print("=" * 60)

print("FIBONACCI RETRACEMENT")

print("=" * 60)

for level, price in levels.items():

    print(

        f"{level:6} {price:.2f}"

    )