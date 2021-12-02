from pyspark.sql import functions as F

#======================================================================================
#                                        BAD                                          #
#======================================================================================
F.when( (F.col('prod_status') == 'Delivered') | (((F.datediff('deliveryDate_actual', 'current_date') < 0) & ((F.col('currentRegistration') != '') | ((F.datediff('deliveryDate_actual', 'current_date') < 0) & ((F.col('originalOperator') != '') | (F.col('currentOperator') != '')))))), 'In Service')


#======================================================================================
#                                        BETTER                                       #
#======================================================================================
has_operator = ((F.col('originalOperator') != '') | (F.col('currentOperator') != ''))
delivery_date_passed = (F.datediff('deliveryDate_actual', 'current_date') < 0)
has_registration = (F.col('currentRegistration').rlike('.+'))
is_delivered = (F.col('prod_status') == 'Delivered')

F.when(is_delivered | (delivery_date_passed & (has_registration | has_operator)), 'In Service')

#======================================================================================
#                                        GOOD                                         #
#======================================================================================
has_operator = ((F.col('originalOperator') != '') | (F.col('currentOperator') != ''))
delivery_date_passed = (F.datediff('deliveryDate_actual', 'current_date') < 0)
has_registration = (F.col('currentRegistration').rlike('.+'))
is_delivered = (F.col('prod_status') == 'Delivered')
is_active = (has_registration | has_operator)

F.when(is_delivered | (delivery_date_passed & is_active), 'In Service')