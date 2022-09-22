-- Sql example

create table op_00
(
	[No] bigint identity primary key,
	[Id] varchar(36),
	[agent_code] nvarchar(50),
	[agent_name] nvarchar(200),
	[description] nvarchar(50),
	[status] int,
	[created_by] varchar(36),
	[created_date] datetime,
	[updated_by] varchar(36),
	[updated_date] datetime
)

