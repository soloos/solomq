package solomq

import (
	"soloos/common/solodbapi"
)

func (p *Solomq) RegisterInDB() error {
	var (
		sess solodbapi.Session
		err  error
	)

	err = p.dbConn.InitSession(&sess)
	if err != nil {
		return err
	}

	err = sess.ReplaceInto("b_solomq_solomq").
		PrimaryColumns("peer_id").PrimaryValues(string(p.srpcPeer.ID[:])).
		Columns("description").Values(p.srpcServer.srpcServerListenAddr).
		Exec()
	if err != nil {
		return err
	}

	return nil
}
