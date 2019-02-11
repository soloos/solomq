package agent

import (
	"database/sql"
	"soloos/common/sdbapi"
	"soloos/common/sdbapitypes"
	"soloos/common/swalapitypes"
)

func (p *TopicDriver) InsertTopicInDB(pTopicMeta *swalapitypes.TopicMeta) error {
	var (
		sess sdbapi.Session
		tx   sdbapi.Tx
		res  sql.Result
		err  error
	)

	err = p.swalAgent.dbConn.InitSessionWithTx(&sess, &tx)
	if err != nil {
		goto QUERY_DONE
	}

	res, err = tx.InsertInto("b_swal_topic").Columns("topic_name").
		Values(pTopicMeta.TopicName.Str()).
		Exec()
	if err != nil {
		goto QUERY_DONE
	}

	pTopicMeta.TopicID, err = res.LastInsertId()
	if err != nil {
		goto QUERY_DONE
	}

	for _, swalMember := range pTopicMeta.SWALMemberGroup.Slice() {
		_, err = tx.InsertInto("r_swal_topic_member").
			Columns("topic_id", "swal_member_peer_id", "role").
			Values(pTopicMeta.TopicID,
				swalMember.PeerID.Str(),
				swalMember.Role).
			Exec()
		if err != nil {
			goto QUERY_DONE
		}
	}

QUERY_DONE:
	if err != nil {
		tx.RollbackUnlessCommitted()
		if sdbapi.IsDuplicateEntryError(err) {
			err = nil
		}
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func (p *TopicDriver) FetchTopicByNameFromDB(topicName string, pTopicMeta *swalapitypes.TopicMeta) error {
	var (
		sess    sdbapi.Session
		sqlRows *sql.Rows
		err     error
	)

	err = p.swalAgent.dbConn.InitSession(&sess)
	if err != nil {
		goto QUERY_DONE
	}

	sqlRows, err = sess.Select("topic_id", "topic_name").
		From("b_swal_topic").
		Where("topic_name=?", topicName).Rows()
	if err != nil {
		goto QUERY_DONE
	}

	if sqlRows.Next() == false {
		err = sdbapitypes.ErrObjectNotExists
		goto QUERY_DONE
	}

	err = sqlRows.Scan(&pTopicMeta.TopicID, &topicName)
	if err != nil {
		goto QUERY_DONE
	}
	pTopicMeta.TopicName.SetStr(topicName)

	sqlRows.Close()

	err = p.fetchTopicMembersFromDB(&sess, pTopicMeta)

QUERY_DONE:
	if sqlRows != nil {
		sqlRows.Close()
	}

	return err
}

func (p *TopicDriver) FetchTopicByIDFromDB(topicID swalapitypes.TopicID, pTopicMeta *swalapitypes.TopicMeta) error {
	var (
		sess      sdbapi.Session
		sqlRows   *sql.Rows
		topicName string
		err       error
	)

	err = p.swalAgent.dbConn.InitSession(&sess)
	if err != nil {
		goto QUERY_DONE
	}

	sqlRows, err = sess.Select("topic_id", "topic_name").
		From("b_swal_topic").
		Where("topic_id=?", topicID).Rows()
	if err != nil {
		goto QUERY_DONE
	}

	if sqlRows.Next() == false {
		err = sdbapitypes.ErrObjectNotExists
		goto QUERY_DONE
	}

	err = sqlRows.Scan(&pTopicMeta.TopicID, &topicName)
	if err != nil {
		goto QUERY_DONE
	}
	pTopicMeta.TopicName.SetStr(topicName)

	sqlRows.Close()

	err = p.fetchTopicMembersFromDB(&sess, pTopicMeta)

QUERY_DONE:
	if sqlRows != nil {
		sqlRows.Close()
	}

	return err
}

func (p *TopicDriver) fetchTopicMembersFromDB(
	sess *sdbapi.Session,
	pTopicMeta *swalapitypes.TopicMeta,
) error {
	var (
		sqlRows    *sql.Rows
		swalMember swalapitypes.SWALMember
		peerIDStr  string
		err        error
	)

	sqlRows, err = sess.Select("swal_member_peer_id", "role").
		From("r_swal_topic_member").
		Where("topic_id=?", pTopicMeta.TopicID).Rows()
	if err != nil {
		goto QUERY_DONE
	}

	for sqlRows.Next() {
		err = sqlRows.Scan(&peerIDStr, &swalMember.Role)
		if err != nil {
			goto QUERY_DONE
		}
		swalMember.PeerID.SetStr(peerIDStr)
		pTopicMeta.SWALMemberGroup.Append(swalMember)
	}

QUERY_DONE:
	if sqlRows != nil {
		sqlRows.Close()
	}

	return err
}
