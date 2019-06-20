package agent

import "soloos/common/log"

func (p *SWALAgent) installSchema(dbDriver string) error {
	var (
		sqls []string
		err  error
	)

	sqls = p.prepareSchemaSqls(dbDriver)
	for _, sql := range sqls {
		_, err = p.dbConn.Exec(sql)
		if err != nil {
			log.Error(err, sql)
		}
	}

	return nil
}

func (p *SWALAgent) prepareSchemaSqls(dbDriver string) []string {
	var sqls []string

	sqls = append(sqls, `
	create table if not exists b_swal_agent (
		peer_id char(64),
		serve_addr varchar(512),
		primary key(peer_id)
	);
	`)

	switch dbDriver {
	case "mysql":
		sqls = append(sqls, `
	create table if not exists b_swal_topic (
		topic_id int auto_increment,
		topic_name char(64),
		primary key(topic_id)
	);
	`)
	case "sqlite":
		sqls = append(sqls, `
	create table if not exists b_swal_topic (
		topic_id int autoincrement,
		topic_name char(64),
		primary key(topic_id)
	);
	`)
	case "postgres":
		sqls = append(sqls, `
	create table if not exists b_swal_topic (
		topic_id serial,
		topic_name char(64),
		primary key(topic_id)
	);
	`)

	}

	sqls = append(sqls, `
	create unique index if not exists i_b_swal_topic_on_name
	on b_swal_topic(topic_name);
	`)

	sqls = append(sqls, `
	create table if not exists r_swal_topic_member (
		topic_id char(64),
		swal_member_peer_id char(64),
		role int,
		primary key(topic_id, swal_member_peer_id)
	);
	`)

	sqls = append(sqls, `
	create index if not exists i_r_swal_topic_member_on_member 
	on r_swal_topic_member(swal_member_peer_id);
	`)

	return sqls
}
