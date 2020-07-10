#pragma once

#include <fcntl.h>
#include <functional>
#include <iostream>
#include <signal.h>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

struct System {
	static void
	profile(const std::string &name, std::function<void()> body)
	{
		std::string filename = name.find(".data") == std::string::npos
			? (name + ".data")
			: name;

		// Launch profiler
		pid_t pid;
		std::stringstream s;
		s << getpid();
		pid = fork();
		if (pid == 0) {
			auto fd = open("/dev/null", O_RDWR);
			dup2(fd, 1);
			dup2(fd, 2);
			exit(execl("/usr/bin/perf", "perf", "record", "-a", "-g", "-o",
				   filename.c_str(), "-p", s.str().c_str(),
				   nullptr));
		}

		// Run body
		body();

		// Kill profiler
		kill(pid, SIGINT);
		waitpid(pid, nullptr, 0);
	}

	static void
	profile(std::function<void()> body)
	{
		profile("perf.data", body);
	}
};