#include "parser.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

static bool
is_builtin_cd(const command &cmd)
{
	return cmd.exe == "cd";
}

static bool
is_builtin_exit(const command &cmd)
{
	return cmd.exe == "exit";
}

static std::vector<char *>
build_argv(const command &cmd)
{
	std::vector<char *> argv;
	argv.reserve(cmd.args.size() + 2);
	argv.push_back(const_cast<char *>(cmd.exe.c_str()));
	for (const std::string &arg : cmd.args)
		argv.push_back(const_cast<char *>(arg.c_str()));
	argv.push_back(nullptr);
	return argv;
}

static void
run_builtin_in_child(const command &cmd)
{
	if (is_builtin_cd(cmd)) {
		if (!cmd.args.empty())
			(void)chdir(cmd.args[0].c_str());
		_exit(0);
	}
	if (is_builtin_exit(cmd)) {
		int code = 0;
		if (!cmd.args.empty())
			code = atoi(cmd.args[0].c_str());
		_exit(code);
	}
	_exit(127);
}

static int
status_to_code(int status)
{
	if (WIFEXITED(status))
		return WEXITSTATUS(status);
	if (WIFSIGNALED(status))
		return 128 + WTERMSIG(status);
	return 0;
}

static int
execute_pipeline(const std::vector<command> &commands,
                 bool apply_redirect,
                 enum output_type out_type,
                 const std::string &out_file,
                 bool allow_shell_exit)
{
	assert(!commands.empty());

	const bool has_pipe = (commands.size() > 1);

	if (!has_pipe) {
		const command &cmd = commands[0];
		if (is_builtin_cd(cmd)) {
			if (cmd.args.empty())
				return 0;
			if (chdir(cmd.args[0].c_str()) != 0) {
				fprintf(stderr, "cd: %s: %s\n",
				        cmd.args[0].c_str(), strerror(errno));
				return 1;
			}
			return 0;
		}
		if (is_builtin_exit(cmd)) {
			int code = 0;
			if (!cmd.args.empty())
				code = atoi(cmd.args[0].c_str());
			if (allow_shell_exit && out_type == OUTPUT_TYPE_STDOUT)
				exit(code);
			return code;
		}
	}

	int out_fd = -1;
	if (apply_redirect &&
	    (out_type == OUTPUT_TYPE_FILE_NEW || out_type == OUTPUT_TYPE_FILE_APPEND)) {
		int flags = O_WRONLY | O_CREAT;
		if (out_type == OUTPUT_TYPE_FILE_NEW)
			flags |= O_TRUNC;
		else
			flags |= O_APPEND;
		out_fd = open(out_file.c_str(), flags, 0666);
		if (out_fd < 0) {
			fprintf(stderr, "open: %s: %s\n",
			        out_file.c_str(), strerror(errno));
			return 1;
		}
	}

	std::vector<int> pipe_fds;
	pipe_fds.resize((commands.size() > 1 ? (commands.size() - 1) * 2 : 0), -1);
	for (size_t i = 0; i + 1 < commands.size(); ++i) {
		int fds[2];
		if (pipe(fds) != 0) {
			fprintf(stderr, "pipe: %s\n", strerror(errno));
			return 1;
		}
		pipe_fds[i * 2] = fds[0];
		pipe_fds[i * 2 + 1] = fds[1];
	}

	std::vector<pid_t> pids;
	pids.reserve(commands.size());

	for (size_t i = 0; i < commands.size(); ++i) {
		pid_t pid = fork();
		if (pid < 0) {
			fprintf(stderr, "fork: %s\n", strerror(errno));
			return 1;
		}
		if (pid == 0) {
			if (i > 0) {
				int read_fd = pipe_fds[(i - 1) * 2];
				dup2(read_fd, STDIN_FILENO);
			}
			if (i + 1 < commands.size()) {
				int write_fd = pipe_fds[i * 2 + 1];
				dup2(write_fd, STDOUT_FILENO);
			} else if (out_fd >= 0) {
				dup2(out_fd, STDOUT_FILENO);
			}

			for (int fd : pipe_fds)
				if (fd >= 0)
					close(fd);
			if (out_fd >= 0)
				close(out_fd);

			const command &cmd = commands[i];
			if (is_builtin_cd(cmd) || is_builtin_exit(cmd)) {
				run_builtin_in_child(cmd);
			}

			std::vector<char *> argv = build_argv(cmd);
			execvp(cmd.exe.c_str(), argv.data());

			fprintf(stderr, "%s: %s\n", cmd.exe.c_str(), strerror(errno));
			_exit(127);
		}

		pids.push_back(pid);
	}

	for (int fd : pipe_fds)
		if (fd >= 0)
			close(fd);
	if (out_fd >= 0)
		close(out_fd);

	int last_status = 0;
	pid_t last_pid = pids.empty() ? -1 : pids.back();
	for (pid_t pid : pids) {
		int status = 0;
		(void)waitpid(pid, &status, 0);
		if (pid == last_pid)
			last_status = status_to_code(status);
	}
	return last_status;
}

static int
execute_command_line_internal(const struct command_line *line)
{
	assert(line != NULL);
	int last_status = 0;

	struct pipeline {
		std::vector<command> commands;
		enum expr_type op_to_next = EXPR_TYPE_COMMAND;
	};
	std::vector<pipeline> pipelines;
	pipeline current;
	bool expect_command = true;
	for (const expr &e : line->exprs) {
		if (expect_command) {
			if (e.type != EXPR_TYPE_COMMAND)
				return 0;
			current.commands.push_back(*e.cmd);
			expect_command = false;
		} else {
			if (e.type == EXPR_TYPE_PIPE) {
				expect_command = true;
				continue;
			}
			if (e.type == EXPR_TYPE_AND || e.type == EXPR_TYPE_OR) {
				current.op_to_next = e.type;
				pipelines.push_back(std::move(current));
				current = pipeline();
				expect_command = true;
				continue;
			}
			return 0;
		}
	}
	if (!current.commands.empty())
		pipelines.push_back(std::move(current));
	if (pipelines.empty())
		return 0;

	for (size_t i = 0; i < pipelines.size(); ++i) {
		bool should_run = true;
		if (i > 0) {
			enum expr_type prev_op = pipelines[i - 1].op_to_next;
			if (prev_op == EXPR_TYPE_AND)
				should_run = (last_status == 0);
			else if (prev_op == EXPR_TYPE_OR)
				should_run = (last_status != 0);
		}

		if (!should_run)
			continue;

		bool apply_redirect = false;
		if (line->out_type != OUTPUT_TYPE_STDOUT && i == pipelines.size() - 1)
			apply_redirect = true;

		last_status = execute_pipeline(
		    pipelines[i].commands,
		    apply_redirect,
		    line->out_type,
		    line->out_file,
		    true);
	}

	return last_status;
}

static int
execute_command_line(const struct command_line *line)
{
	assert(line != NULL);

	while (true) {
		int status = 0;
		pid_t pid = waitpid(-1, &status, WNOHANG);
		if (pid <= 0)
			break;
	}

	if (line->is_background) {
		pid_t bg = fork();
		if (bg < 0) {
			fprintf(stderr, "fork: %s\n", strerror(errno));
			return 1;
		}
		if (bg == 0) {
			int code = execute_command_line_internal(line);
			_exit(code);
		}
		return 0;
	}

	return execute_command_line_internal(line);
}

int
main(void)
{
	const size_t buf_size = 1024;
	char buf[buf_size];
	int rc;
	struct parser *p = parser_new();
	int last_status = 0;
	while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
		parser_feed(p, buf, rc);
		struct command_line *line = NULL;
		while (true) {
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				printf("Error: %d\n", (int)err);
				continue;
			}
			last_status = execute_command_line(line);
			delete line;
		}
	}
	parser_delete(p);
	return last_status;
}
