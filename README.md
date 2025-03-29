Wrapper methods for pyspark functionalities which:
1. Simplify existing pyspark commands by using preset default values for certain pyspark functions
2. Don't exist at the moment in native pyspark

Wrapper functions/methods provided in this repo, include those that help in:
1. Reading/writing datasets
2. Calculating summary statistics
3. Making single pyspark df method call instead of mulitple calls with native pyspark functions (like using a single .withColumnsOrdered call instead of multiple .withColumns)
