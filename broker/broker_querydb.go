package broker

import (
	"soloos/common/solodbapi"
)

func (p *Broker) RegisterInDB() error {
	var (
		sess solodbapi.Session
		err  error
	)

	err = p.dbConn.InitSession(&sess)
	if err != nil {
		return err
	}

	err = sess.ReplaceInto("b_solomq_broker").
		PrimaryColumns("peer_id").PrimaryValues(string(p.srpcPeer.ID[:])).
		Columns("description").Values(p.srpcServer.srpcServerListenAddr).
		Exec()
	if err != nil {
		return err
	}

	return nil
}
