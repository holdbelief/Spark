package com.bjsxt.spark.dao;

import com.bjsxt.spark.domain.Task;

public interface ITaskDAO {

	/**
	 * 根据task的主键查询指定的任务
	 * @param taskId
	 * @return
	 */
	Task findTaskById(long taskId);
}
