sql2lang
=======
a simple sql 'create table' statement 2 language class/structure definition converter .

Usage
----

```
sql2lang rust 'CREATE TABLE public.historical_data_template (
                    date date NOT NULL,
                    open double precision NOT NULL,
                    high double precision NOT NULL,
                    low double precision NOT NULL,
                    close double precision NOT NULL,
                    volume bigint,
                    CONSTRAINT historical_data_template_pk PRIMARY KEY (date)
                
                );'
```

or 

```
sql2lang python 'CREATE TABLE test {
			bla date NOT NULL,
		};'
```


Status
----
Currently only rust/diesel is partially implemented. 
