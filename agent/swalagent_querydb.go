package agent

import (
	"soloos/common/sdbapi"
)

func (p *SWALAgent) RegisterInDB() error {
	var (
		sess sdbapi.Session
		err  error
	)

	err = p.dbConn.InitSession(&sess)
	if err != nil {
		return err
	}

	err = sess.ReplaceInto("b_swal_agent").
		PrimaryColumns("peer_id").PrimaryValues(string(p.peer.ID[:])).
		Columns("serve_addr").Values(p.srpcServer.srpcServerListenAddr).
		Exec()
	if err != nil {
		return err
	}

	return nil
}
