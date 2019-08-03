package broker

import (
	"soloos/common/sdbapi"
)

func (p *Broker) RegisterInDB() error {
	var (
		sess sdbapi.Session
		err  error
	)

	err = p.dbConn.InitSession(&sess)
	if err != nil {
		return err
	}

	err = sess.ReplaceInto("b_swal_broker").
		PrimaryColumns("peer_id").PrimaryValues(string(p.srpcPeer.ID[:])).
		Columns("desc").Values(p.srpcServer.srpcServerListenAddr).
		Exec()
	if err != nil {
		return err
	}

	return nil
}
