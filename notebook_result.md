

```
 ------ INICIA ETL DE PROVEEDORES -----
+------------+--------------------+---------+-------------+------------+----------+
|ID_Proveedor|              Nombre|Dias_pago|Codigo_postal|ID_Categoria|ID_Persona|
+------------+--------------------+---------+-------------+------------+----------+
|           4|      Fabrikam, Inc.|       30|        40351|           4|        27|
|           5|Graphic Design In...|       14|        64847|           2|        29|
|           7|       Litware, Inc.|       30|        95245|           5|        33|
|           9|      Nod Publishers|        7|        27906|           2|        37|
|          10|Northwind Electri...|       30|         7860|           3|        39|
+------------+--------------------+---------+-------------+------------+----------+
only showing top 5 rows

+-------------------+------------+
|          Categoria|ID_Categoria|
+-------------------+------------+
|     otro mayorista|           1|
|productos novedosos|           2|
|           juguetes|           3|
|               ropa|           4|
|           embalaje|           5|
+-------------------+------------+
only showing top 5 rows

+----------+--------------------+
|ID_Persona|  Contacto_principal|
+----------+--------------------+
|         1|Data Conversion Only|
|         2|      Kayla Woodcock|
|         3|       Hudson Onslow|
|         4|       Isabella Rupp|
|         5|         Eva Muirden|
+----------+--------------------+
only showing top 5 rows

+------------+------------+--------------------+---------+-------------+----------+-------------------+
|ID_Categoria|ID_Proveedor|              Nombre|Dias_pago|Codigo_postal|ID_Persona|          Categoria|
+------------+------------+--------------------+---------+-------------+----------+-------------------+
|           2|           5|Graphic Design In...|       14|        64847|        29|productos novedosos|
|           2|           9|      Nod Publishers|        7|        27906|        37|productos novedosos|
|           2|          12|   The Phone Company|       30|        56732|        43|productos novedosos|
|           2|           2|       Contoso, Ltd.|       -7|        98253|        23|productos novedosos|
|           2|           8|  Lucerne Publishing|      -30|        37659|        35|productos novedosos|
+------------+------------+--------------------+---------+-------------+----------+-------------------+
only showing top 5 rows

+----------+------------+------------+-------------------+---------+-------------+--------------------+------------------+
|ID_Persona|ID_Categoria|ID_Proveedor|             Nombre|Dias_pago|Codigo_postal|           Categoria|Contacto_principal|
+----------+------------+------------+-------------------+---------+-------------+--------------------+------------------+
|        31|           9|           6|Humongous Insurance|      -14|        37770|servicios de seguros|Madelaine  Cartier|
|        27|           4|           4|     Fabrikam, Inc.|       30|        40351|                ropa|       Bill Lawson|
|        41|           8|          11|      Trey Research|       -7|        57543|servicios de mark...|      Donald Jones|
|        43|           2|          12|  The Phone Company|       30|        56732| productos novedosos|           Hai Dam|
|        37|           2|           9|     Nod Publishers|        7|        27906| productos novedosos|      Marcos Costa|
+----------+------------+------------+-------------------+---------+-------------+--------------------+------------------+
only showing top 5 rows

+------------+--------------------+---------+-------------+--------------------+------------------+
|ID_Proveedor|              Nombre|Dias_pago|Codigo_postal|           Categoria|Contacto_principal|
+------------+--------------------+---------+-------------+--------------------+------------------+
|           5|Graphic Design In...|       14|        64847| productos novedosos|        Penny Buck|
|           3|Consolidated Mess...|      -30|        94101|servicios de mens...|      Kerstin Parn|
|           2|       Contoso, Ltd.|       -7|        98253| productos novedosos|   Hanna Mihhailov|
|           9|      Nod Publishers|        7|        27906| productos novedosos|      Marcos Costa|
|           6| Humongous Insurance|      -14|        37770|servicios de seguros|Madelaine  Cartier|
+------------+--------------------+---------+-------------+--------------------+------------------+
only showing top 5 rows

------ FINALIZA ETL DE PROVEEDORES -----
------ INICIA ETL DE TIPOS DE TRANSACCION -----
+-------------------+--------------------+
|ID_Tipo_transaccion|                Tipo|
+-------------------+--------------------+
|                  2|Customer Credit Note|
|                  3|Customer Payment ...|
|                  4|     Customer Refund|
|                  5|    Supplier Invoice|
|                  6|Supplier Credit Note|
+-------------------+--------------------+
only showing top 5 rows

------ FINALIZA ETL DE TIPOS DE TRANSACCION -----
------ INICIA ETL DE MOVIMIENTOS -----
+------------+-----------+-------------------+----------+--------+----------------+
|ID_proveedor|ID_Producto|ID_Tipo_transaccion|ID_Cliente|Cantidad|Fecha_movimiento|
+------------+-----------+-------------------+----------+--------+----------------+
|       94344|        108|                 10|     185.0|   -10.0|     Jan 20,2014|
|       96548|        162|                 10|     176.0|   -10.0|     Jan 28,2014|
|       96560|        216|                 10|     474.0|   -10.0|     Jan 28,2014|
|       96568|         22|                 10|     901.0|   -10.0|     Jan 28,2014|
|       96648|         25|                 10|     926.0|   -10.0|     Jan 28,2014|
+------------+-----------+-------------------+----------+--------+----------------+
only showing top 5 rows

2000 0
+------------+-----------+-------------------+----------+--------+----------------+
|ID_proveedor|ID_Producto|ID_Tipo_transaccion|ID_Cliente|Cantidad|Fecha_movimiento|
+------------+-----------+-------------------+----------+--------+----------------+
|       94344|        108|                 10|     185.0|   -10.0|     Jan 20,2014|
|       96548|        162|                 10|     176.0|   -10.0|     Jan 28,2014|
|       96560|        216|                 10|     474.0|   -10.0|     Jan 28,2014|
|       96568|         22|                 10|     901.0|   -10.0|     Jan 28,2014|
|       96648|         25|                 10|     926.0|   -10.0|     Jan 28,2014|
+------------+-----------+-------------------+----------+--------+----------------+
only showing top 5 rows

None
+------------+-----------+-------------------+----------+--------+----------------+
|ID_proveedor|ID_Producto|ID_Tipo_transaccion|ID_Cliente|Cantidad|Fecha_movimiento|
+------------+-----------+-------------------+----------+--------+----------------+
|      120968|        155|                 10|       2.0|   -10.0|      2014-05-02|
|      167083|         55|                 10|     513.0|   -10.0|      2014-10-21|
|      243458|         71|                 10|     566.0|   -10.0|      2015-07-14|
|      286524|         74|                 10|     906.0|   -10.0|      2015-12-09|
|      335407|         45|                 10|     195.0|   -10.0|      2016-05-28|
+------------+-----------+-------------------+----------+--------+----------------+
only showing top 5 rows

[Stage 59:>                                                         (0 + 1) / 1]
------ FINALIZA ETL DE MOVIMIENTOS -----
```
