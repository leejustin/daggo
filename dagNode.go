package daggo

import (
	"database/sql"
	"fmt"
)

func (d *Daggo) GetNodeByID(nodeID int) (*DagNode, error) {
	var node DagNode

	query := "SELECT * FROM dag WHERE id = $1"
	err := d.db.Get(&node, query, nodeID)
	if err == sql.ErrNoRows {
		return nil, nil // No node found
	} else if err != nil {
		return nil, fmt.Errorf("failed to get node: %v", err)
	}

	return &node, nil
}

// GetNextChildrenNodes GetNode returns the immediate children nodes of the given node ID
func (d *Daggo) GetNextChildrenNodes(nodeID int) ([]DagNode, error) {
	dagNodes := make([]DagNode, 0)

	query := "SELECT * FROM dag WHERE parent_id = $1 ORDER BY id ASC"
	err := d.db.Select(&dagNodes, query, nodeID)
	if err != nil {
		return nil, err
	}

	if dagNodes == nil {
		return []DagNode{}, nil
	} else {
		return dagNodes, nil
	}
}

// GetParentNode returns the immediate parent node of the given node
func (d *Daggo) GetParentNode(nodeID int) (*DagNode, error) {
	var node DagNode

	// Query the database for the parent of the node with the given nodeID
	query := "SELECT * FROM dag WHERE child_id = $1"
	err := d.db.Get(&node, query, nodeID)
	if err == sql.ErrNoRows {
		return nil, nil // No parent node found when it's the root node
	} else if err != nil {
		return nil, fmt.Errorf("failed to get parent node: %v", err)
	}

	return &node, nil
}

// GetRootNode returns the root node of the given node
func (d *Daggo) GetRootNode(nodeID int) (*DagNode, error) {
	var node DagNode

	query := "SELECT * FROM dag WHERE root_id = $1"
	err := d.db.Get(&node, query, nodeID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("no root node found for node %d", nodeID)
	} else if err != nil {
		return nil, err
	}
	return &node, nil
}

// GetDescendants returns all descendants of the given node ID
func (d *Daggo) GetDescendants(nodeID int) ([]DagNode, error) {
	descendants := make([]DagNode, 0)

	query := `
		WITH RECURSIVE cte AS (
			SELECT *
			FROM dag
			WHERE parent_id = $1
			UNION ALL
			SELECT dag.*
			FROM dag
			JOIN cte ON dag.parent_id = cte.child_id
		)
		SELECT DISTINCT d.*
		FROM cte
		JOIN unnest(cte.child_id) AS c ON d.id = c
	`

	// Execute the query and retrieve the descendants
	err := d.db.Select(&descendants, query, nodeID)
	if err != nil {
		return nil, err
	}

	if descendants == nil {
		return []DagNode{}, nil
	} else {
		return descendants, nil
	}
}

// GetAncestors returns all ancestors of the given node ID
func (d *Daggo) GetAncestors(nodeID int) ([]DagNode, error) {
	ancestors := make([]DagNode, 0)

	query := `
		WITH RECURSIVE cte AS (
			SELECT *
			FROM dag
			WHERE child_id = $1
			UNION ALL
			SELECT dag.*
			FROM dag
			JOIN cte ON dag.child_id = cte.parent_id
		)
		SELECT DISTINCT d.*
		FROM cte
		JOIN unnest(cte.parent_id) AS p ON d.id = p
	`

	// Execute the query and retrieve the ancestors
	err := d.db.Select(&ancestors, query, nodeID)
	if err != nil {
		return nil, err
	}

	if ancestors == nil {
		return []DagNode{}, nil
	} else {
		return ancestors, nil
	}
}

// AddChildNode creates a new node with the given ID and parent ID
func (d *Daggo) AddChildNode(id int, parentID int) error {
	// Check if node with given ID already exists in the database
	existingNode, err := d.GetNodeByID(id)
	if err != nil {
		return err
	}
	if existingNode != nil {
		return fmt.Errorf("node with ID %d already exists", id)
	}

	// Get root ID for new node
	parentNode, err := d.GetParentNode(parentID)
	if err != nil {
		return err
	}
	rootID := parentNode.RootID

	// Insert new node into database
	query := "INSERT INTO dag (id, parent_id, root_id) VALUES ($1, $2, $3)"
	_, err = d.db.Exec(query, id, parentID, rootID)
	if err != nil {
		return fmt.Errorf("failed to add child node: %v", err)
	}

	return nil
}

// AddRootNode creates a new root node with the given ID
func (d *Daggo) AddRootNode(id int) error {
	// Check if node with given ID already exists in the database
	existingNode, err := d.GetNodeByID(id)
	if err != nil {
		return err
	}
	if existingNode != nil {
		return fmt.Errorf("node with ID %d already exists", id)
	}

	// Insert new root node into database
	query := "INSERT INTO dag (id, parent_id, root_id) VALUES ($1, NULL, $1)"
	_, err = d.db.Exec(query, id)
	if err != nil {
		return fmt.Errorf("failed to add root node: %v", err)
	}

	return nil
}

// DeleteChildNode deletes the node with the given ID and removes it from its parent's ChildIDs list
func (d *Daggo) DeleteChildNode(nodeId int) error {
	// Start a transaction
	tx, err := d.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		// Rollback the transaction if it failed to commit
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		}
	}()

	// Get the node with the given ID
	node := &DagNode{}
	err = tx.Get(node, "SELECT * FROM DagNode WHERE ID = $1", nodeId)
	if err != nil {
		return fmt.Errorf("failed to get node: %v", err)
	}

	if node.GetChildIDs() != nil || len(node.GetChildIDs()) > 0 {
		return fmt.Errorf("cannot delete node with children")
	}

	// Delete the node
	_, err = tx.Exec("DELETE FROM DagNode WHERE ID = $1", nodeId)
	if err != nil {
		return fmt.Errorf("failed to delete node: %v", err)
	}

	// If the node had a parent, update its ChildIDs list
	if node.ParentID.Valid {
		// Get the parent node
		parent := &DagNode{}
		err = tx.Get(parent, "SELECT * FROM DagNode WHERE ID = $1", node.ParentID.Int64)
		if err != nil {
			return fmt.Errorf("failed to get parent node: %v", err)
		}

		// Remove the deleted node's ID from the parent's ChildIDs list
		newChildIDs := make([]int, 0, len(parent.ChildIDs)-1)
		for _, childID := range parent.ChildIDs {
			if childID != nodeId {
				newChildIDs = append(newChildIDs, childID)
			}
		}
		_, err = tx.Exec("UPDATE DagNode SET ChildIDs = $1 WHERE ID = $2", newChildIDs, parent.ID)
		if err != nil {
			return fmt.Errorf("failed to update parent node: %v", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}

// DeleteNodeAndDescendants deletes the node with the given ID and all of its descendants
func (d *Daggo) DeleteNodeAndDescendants(nodeID int) error {
	// Start a transaction
	tx, err := d.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer func() {
		// Rollback the transaction if it failed to commit
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		} else if err != nil {
			tx.Rollback()
		}
	}()

	// Recursive query to delete the node and its descendants
	query := `
		WITH RECURSIVE cte AS (
			SELECT child_id
			FROM dag
			WHERE parent_id = $1
			UNION ALL
			SELECT dag.child_id
			FROM dag
			JOIN cte ON dag.parent_id = any(cte.child_id)
		)
		DELETE FROM dag
		WHERE child_id @> (SELECT array_agg(child_id) FROM cte)
	`

	// Execute the recursive delete query
	_, err = tx.Exec(query, nodeID)
	if err != nil {
		return fmt.Errorf("failed to delete node and descendants: %v", err)
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %v", err)
	}

	return nil
}
