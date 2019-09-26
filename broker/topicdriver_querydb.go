package broker

import (
	"database/sql"
	"soloos/common/solodbapi"
	"soloos/common/solodbapitypes"
	"soloos/common/solomqapitypes"
)

func (p *TopicDriver) InsertTopicInDB(pTopicMeta *solomqapitypes.TopicMeta) error {
	var (
		sess solodbapi.Session
		tx   solodbapi.Tx
		res  sql.Result
		err  error
	)

	err = p.broker.dbConn.InitSessionWithTx(&sess, &tx)
	if err != nil {
		goto QUERY_DONE
	}

	res, err = tx.InsertInto("b_solomq_topic").Columns("topic_name").
		Values(pTopicMeta.TopicName.Str()).
		Exec()
	if err != nil {
		goto QUERY_DONE
	}

	pTopicMeta.TopicID, err = res.LastInsertId()
	if err != nil {
		goto QUERY_DONE
	}

	for _, solomqMember := range pTopicMeta.SOLOMQMemberGroup.Slice() {
		_, err = tx.InsertInto("r_solomq_topic_member").
			Columns("topic_id", "solomq_member_peer_id", "role").
			Values(pTopicMeta.TopicID,
				solomqMember.PeerID.Str(),
				solomqMember.Role).
			Exec()
		if err != nil {
			goto QUERY_DONE
		}
	}

QUERY_DONE:
	if err != nil {
		tx.RollbackUnlessCommitted()
		if solodbapi.IsDuplicateEntryError(err) {
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

func (p *TopicDriver) FetchTopicByNameFromDB(topicName string, pTopicMeta *solomqapitypes.TopicMeta) error {
	var (
		sess    solodbapi.Session
		sqlRows *sql.Rows
		err     error
	)

	err = p.broker.dbConn.InitSession(&sess)
	if err != nil {
		goto QUERY_DONE
	}

	sqlRows, err = sess.Select("topic_id", "topic_name").
		From("b_solomq_topic").
		Where("topic_name=?", topicName).Rows()
	if err != nil {
		goto QUERY_DONE
	}

	if sqlRows.Next() == false {
		err = solodbapitypes.ErrObjectNotExists
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

func (p *TopicDriver) FetchTopicByIDFromDB(topicID solomqapitypes.TopicID, pTopicMeta *solomqapitypes.TopicMeta) error {
	var (
		sess      solodbapi.Session
		sqlRows   *sql.Rows
		topicName string
		err       error
	)

	err = p.broker.dbConn.InitSession(&sess)
	if err != nil {
		goto QUERY_DONE
	}

	sqlRows, err = sess.Select("topic_id", "topic_name").
		From("b_solomq_topic").
		Where("topic_id=?", topicID).Rows()
	if err != nil {
		goto QUERY_DONE
	}

	if sqlRows.Next() == false {
		err = solodbapitypes.ErrObjectNotExists
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
	sess *solodbapi.Session,
	pTopicMeta *solomqapitypes.TopicMeta,
) error {
	var (
		sqlRows    *sql.Rows
		solomqMember solomqapitypes.SOLOMQMember
		peerIDStr  string
		err        error
	)

	sqlRows, err = sess.Select("solomq_member_peer_id", "role").
		From("r_solomq_topic_member").
		Where("topic_id=?", pTopicMeta.TopicID).Rows()
	if err != nil {
		goto QUERY_DONE
	}

	for sqlRows.Next() {
		err = sqlRows.Scan(&peerIDStr, &solomqMember.Role)
		if err != nil {
			goto QUERY_DONE
		}
		solomqMember.PeerID.SetStr(peerIDStr)
		pTopicMeta.SOLOMQMemberGroup.Append(solomqMember)
	}

QUERY_DONE:
	if sqlRows != nil {
		sqlRows.Close()
	}

	return err
}
