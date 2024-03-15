create procedure syn.usp_ImportFileCustomerSeasonal
	@ID_Record int
-- 1. Ключевое слово для алиаса пишется со строчной буквой
AS
set nocount on
begin
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal)
	/* 
		2. Не нужен еще один declare (для объявление используется, есди необходимо использовать раннее переменную). 
		3. Не найдена переменная с системным названием max
	*/
	declare @ErrorMessage varchar(max)
	-- 4. Комментарий должен быть на одном уровне с условием
-- Проверка на корректность загрузки
	if not exists (
	-- 5. Нет смещения блока кода на 1 отступ
	select 1
	-- 6. Неправильно объяевлен алиас для "syn.ImportFile" 
	from syn.ImportFile as f
	where f.ID = @ID_Record
		and f.FlagLoaded = cast(1 as bit)
	)
		-- 7. Оператор begin не на одном уровне с if
		begin
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'
			raiserror(@ErrorMessage, 3, 1)
			-- 8. Отстутствует пустая строка перед return
			return
		end

	-- Чтение из слоя временных данных
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	-- 9. Нет ключевого слово as перед "cs" 
	from syn.SA_CustomerSeasonal cs
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			and c_dist.ID_mapping_DataSource = 1
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной
	select
		cs.*
		,case
			-- 10. Отсутствуют отступы у then под when
			when c.ID is null then 'UID клиента отсутствует в справочнике "Клиент"'
			when c_dist.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
		and c.ID_mapping_DataSource = 1
	-- 11. Отсутсвует перенос ключевого слова and  
	left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	-- 12. Id имеющий первичный ключ не может быть null
	where c.ID is null
		or c_dist.ID is null
		or s.ID is null
		or cst.ID is null
		-- 13. Название алиаса не может быть системным словом
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	merge into syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		-- 14. Не может таблица иметь префикс #  
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	when matched
		-- 15. Дополнительные условия должны оставаться на строке с when 
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update
		set ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		-- 16. Отсутствует запись вида: into "TableName"
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive);

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)
		raiserror(@ErrorMessage, 1, 1)
		-- 17. Нет отступа в комментарии
		--Формирование таблицы для отчетности
		select top 100
			-- 18. Неправильно объявлены алиасы
			bir.Season as 'Сезон'
			,bir.UID_DS_Customer as 'UID Клиента'
			,bir.Customer as 'Клиент'
			,bir.CustomerSystemType as 'Тип клиента'
			,bir.UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,bir.CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(bir.DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateBegin) as 'Дата начала'
			,isnull(format(try_cast(birDateEnd as date), 'dd.MM.yyyy', 'ru-RU'), bir.DateEnd) as 'Дата окончания'
			,bir.FlagActive as 'Активность'
			,bir.Reason as 'Причина'
		from #BadInsertedRows as bir

		return
	end
end
